package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.*;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
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

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceBindingService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.kc = new krbClient(clusterConfig);
        this.etcdClient = clusterConfig.getEtcdClient();
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
        if(! planId.equals(OCDPAdminServiceMapper.getOCDPServicePlan(serviceDefinitionId))){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }
        Map<String, String> serviceInstanceCredentials = instance.getServiceInstanceCredentials();
        String appGuid = request.getBoundAppGuid();
        return createServiceInstanceBindingWithProvisionUser(
                serviceDefinitionId, bindingId, serviceInstanceId, planId, appGuid, serviceInstanceCredentials);
	}

    /**
     * Create service instance binding by reuse provsion user
     * @param serviceDefinitionId
     * @param bindingId
     * @param serviceInstanceId
     * @param planId
     * @param appGuid
     * @param serviceInstanceCredentials
     * @return
     */
    private CreateServiceInstanceBindingResponse createServiceInstanceBindingWithProvisionUser(
            String serviceDefinitionId, String bindingId, String serviceInstanceId, String planId, String appGuid, Map<String, String> serviceInstanceCredentials){
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // Save service instance binding
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(
                serviceInstanceCredentials.get("username"),
                serviceInstanceCredentials.get("password"),
                serviceInstanceCredentials.get("keytab"),
                serviceInstanceCredentials.get("name"),
                serviceInstanceCredentials.get("rangerPolicyId"));
        ServiceInstanceBinding binding = new ServiceInstanceBinding(
                bindingId, serviceInstanceId, credentials, null, appGuid, planId);
        bindingRepository.save(binding);

        return new CreateServiceInstanceAppBindingResponse().withCredentials(credentials);
    }

    /**
     * Create service instance binding by create new binding user
     * @param serviceDefinitionId
     * @param bindingId
     * @param serviceInstanceId
     * @param planId
     * @param appGuid
     * @param serviceInstanceCredentials
     * @return
     */
    private CreateServiceInstanceBindingResponse createServiceInstanceBindingWithNewUsr(
            String serviceDefinitionId, String bindingId, String serviceInstanceId, String planId, String appGuid, Map<String, String> serviceInstanceCredentials){
        String policyId = serviceInstanceCredentials.get("rangerPolicyId");
        String serviceInstanceResource = serviceInstanceCredentials.get("name");

        String ldapGroupName = this.clusterConfig.getLdapGroup();
        String ldapGroupId = this.clusterConfig.getLdapGroupId();
        String krbRealm = this.clusterConfig.getKrbRealm();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        // Create LDAP user for OCDP service instance binding
        logger.info("create service binding ldap user.");
        String accountName = "binding_" + BrokerUtil.generateAccountName();
        try{
            BrokerUtil.createLDAPUser(this.ldap, this.etcdClient, accountName, ldapGroupName, ldapGroupId);
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
                BrokerUtil.removeLDAPUser(this.ldap, accountName);
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
                BrokerUtil.removeLDAPUser(this.ldap, accountName);
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
        // Save service instance binding
        Map<String, Object> credentials = ocdp.generateCredentialsInfo(
                accountName, pwd, keyTabString, serviceInstanceResource, policyId);
        ServiceInstanceBinding binding = new ServiceInstanceBinding(
                bindingId, serviceInstanceId, credentials, null, appGuid, planId);
        bindingRepository.save(binding);

        credentials.remove("rangerPolicyId");
        return new CreateServiceInstanceAppBindingResponse().withCredentials(credentials);
    }

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
            throws OCDPServiceException{
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String bindingId = request.getBindingId();
        String planId = request.getPlanId();
        ServiceInstanceBinding binding = bindingRepository.findOne(serviceInstanceId, bindingId);
        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }else if(! planId.equals(binding.getPlanId())){
            throw new ServiceBrokerInvalidParametersException("Unknown plan id: " + planId);
        }
        deleteServiceInstancceBindingWithoutUser(serviceInstanceId, bindingId);
    }

    /**
     * Only delete service instance binding info from repository/etcd
     * @param serviceInstanceId
     * @param bindingId
     */
    private void deleteServiceInstancceBindingWithoutUser(String serviceInstanceId, String bindingId){
        bindingRepository.delete(serviceInstanceId, bindingId);
    }

    /**
     * Delete binding LDAP user/princ, remove binding user from ranger policy, delete service instance binding info
     * @param serviceDefinitionId
     * @param serviceInstanceId
     * @param bindingId
     * @param credentials
     */
    private void deleteServiceInstanceBindingWithUser(String serviceDefinitionId, String serviceInstanceId, String bindingId, Map<String, Object> credentials){
        String accountName = (String)credentials.get("username");
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
            BrokerUtil.removeLDAPUser(this.ldap, accountName);
        }catch (Exception e){
            logger.error("Delete LDAP user fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete LDAP user fail due to: " + e.getLocalizedMessage());
        }
        bindingRepository.delete(serviceInstanceId, bindingId);
    }

}
