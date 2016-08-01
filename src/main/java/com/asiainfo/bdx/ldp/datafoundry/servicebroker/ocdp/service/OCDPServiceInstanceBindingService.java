package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.*;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;

import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.service.ServiceInstanceBindingService;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerInvalidParametersException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.support.LdapNameBuilder;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OCDP impl to bind hadoop services.  Binding a service does the following:
 * creates a new user for service instance binding,
 * binding service instance,
 * saves the ServiceInstanceBinding info to the hadoop repository.
 *  
 * @author whitebai1986@gmail.com
 */
@Service
public class OCDPServiceInstanceBindingService implements ServiceInstanceBindingService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceBindingService.class);

    @Autowired
    private OCDPServiceInstanceRepository repository;

    @Autowired
	private OCDPServiceInstanceBindingRepository bindingRepository;

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private LdapTemplate ldap;

    private krbClient kc;

    @Autowired
    public OCDPServiceInstanceBindingService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.kc = new krbClient(clusterConfig);
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

	@Override
	public CreateServiceInstanceBindingResponse createServiceInstanceBinding(CreateServiceInstanceBindingRequest request)
            throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
		String bindingId = request.getBindingId();
		String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstanceBinding binding = bindingRepository.findOne(serviceInstanceId, bindingId);
        if (binding != null) {
           throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
        }
        String planId = request.getPlanId();
        if(!planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }
        Map<String, String> serviceInstanceCredentials = instance.getServiceInstanceCredentials();
        String policyId = serviceInstanceCredentials.get("rangerPolicyId");

        String ldapGroupName = this.clusterConfig.getLdapGroup();
        String krbRealm = this.clusterConfig.getKrbRealm();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        // Create LDAP user for OCDP service instance binding
        logger.info("create service binding ldap user.");
        String accountName = "servicebinding_" + UUID.randomUUID().toString();
        try{
            this.createLDAPUser(accountName, ldapGroupName);
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }

        // Create kerberos principal for OCDP service instance binding
        logger.info("create service binding kerberos principal.");
        String pn = accountName + "@" + krbRealm;
        String pwd = UUID.randomUUID().toString();
        String keyTabString;
        try{
            this.kc.createPrincipal(pn, pwd);
            keyTabString = this.kc.createKeyTabString(pn, pwd, null);
        }catch(KerberosOperationException e){
            logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            logger.info("Rollback LDAP user: " + accountName);
            try{
                this.removeLDAPUser(accountName);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
        }

        // Add service binding user to ranger policy
        int i=0;
        boolean updateResult = false;
        while(i++ <= 20)
        {
            logger.info("Append user " + accountName + " to ranger policy " + policyId);
            updateResult = ocdp.appendUserToResourcePermission(policyId, ldapGroupName, accountName);
            if (updateResult == false){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
            else{
                logger.info("Policy update complete!");
                break;
            }
        }
        if(updateResult == false) {
            logger.error("Fail to Append user " + accountName + " to ranger policy " + policyId);
            logger.info("Rollback LDAP user: " + accountName);
            try {
                this.removeLDAPUser(accountName);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            logger.info("Rollback kerberos principal: " + accountName);
            try {
                this.kc.removePrincipal(pn);
            } catch (KerberosOperationException ex) {
                ex.printStackTrace();
            }
            throw new OCDPServiceException("OCDP service binding fail.");
        }
        Map<String, Object> credentials = new HashMap<String, Object>();
        credentials.put("serviceInstanceBingingUser", accountName);
        credentials.put("serviceInstanceBingingPwd", pwd);
        credentials.put("serviceInstanceBindingKeytab", keyTabString);
        credentials.put("rangerPolicyId", policyId);

        binding = new ServiceInstanceBinding(
                bindingId, serviceInstanceId, credentials, null, request.getBoundAppGuid(), planId);

        bindingRepository.save(binding);

        return new CreateServiceInstanceAppBindingResponse().withCredentials(credentials);
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
            throws OCDPServiceException{
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String bindingId = request.getBindingId();
        String planId = request.getPlanId();
        ServiceInstanceBinding binding = getServiceInstanceBinding(serviceInstanceId, bindingId);
        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }else if(!planId.equals(binding.getPlanId())){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }

        Map<String, Object> credentials = binding.getCredentials();
        String accountName = (String)credentials.get("serviceInstanceBingingUser");
        String policyId = (String)credentials.get("rangerPolicyId");
        String ldapGroupName = this.clusterConfig.getLdapGroup();
        String krbRealm = this.clusterConfig.getKrbRealm();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        // Remove service binding user from Ranger policy
        logger.info("Remove user " + accountName + " from ranger policy " + policyId);
        if (! ocdp.removeUserFromResourcePermission(policyId, ldapGroupName, accountName))
        {
            logger.error("Fail to remove user " + accountName + " to ranger policy " + policyId);
            throw new OCDPServiceException("Ranger policy update fail.");
        }
        // Delete kerberos principal for OCDP service instance binding
        logger.info("Delete service binding kerberos principal.");
        try{
            this.kc.removePrincipal(accountName + "@" + krbRealm);
        }catch(KerberosOperationException e){
            logger.error("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
        }
        // Delete LDAP user for OCDP service instance binding
        logger.info("Delete service binding ldap user.");
        try{
            this.removeLDAPUser(accountName);
        }catch (Exception e){
            logger.error("Delete LDAP user fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete LDAP user fail due to: " + e.getLocalizedMessage());
        }
        bindingRepository.delete(serviceInstanceId, bindingId);
    }

	protected ServiceInstanceBinding getServiceInstanceBinding(String serviceInstanceId, String bindingId) {
		return bindingRepository.findOne(serviceInstanceId, bindingId);
	}

    private void createLDAPUser(String accountName, String groupName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        Attributes userAttributes = new BasicAttributes();
        userAttributes.put("memberOf", "cn=" + groupName + ",ou=Group,dc=asiainfo,dc=com");
        BasicAttribute classAttribute = new BasicAttribute("objectClass");
        classAttribute.add("account");
        userAttributes.put(classAttribute);
        this.ldap.bind(ldapName, null, userAttributes);
    }

    private void removeLDAPUser(String accountName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        this.ldap.unbind(ldapName);
    }

}
