package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.*;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;

import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.service.ServiceInstanceBindingService;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingDoesNotExistException;
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
	private OCDPServiceInstanceBindingRepository bindingRepository;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ClusterConfig clusterConfig;

    public OCDPServiceInstanceBindingService() {}

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
        ServiceInstanceBinding binding = bindingRepository.findOne(serviceDefinitionId, bindingId);
        if (binding != null) {
           throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
        }

        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        // Create LDAP user for OCDP service instance binding
        logger.info("create service binding ldap user.");
        LdapTemplate ldap = this.clusterConfig.getLdapTemplate();
        String accountName = "serviceBinding_" + UUID.randomUUID().toString();
        try{
            this.createLDAPUser(ldap, accountName, clusterConfig.getLdapGroup());
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }

        // Create kerberos principal for OCDP service instance binding
        logger.info("create service binding kerberos principal.");
        krbClient kc = new krbClient(this.clusterConfig);
        String pn = accountName +  "@ASIAINFO.COM";
        String pwd = UUID.randomUUID().toString();
        String keyTabString;
        try{
            kc.createPrincipal(pn, pwd);
            keyTabString = kc.createKeyTabString(pn, pwd, null);
        }catch(KerberosOperationException e){
            logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            logger.info("Rollback LDAP user: " + accountName);
            try{
                this.removeLDAPUser(ldap, accountName);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
        }
        // Create Hadoop resource like hdfs folder, hbase table ...
        String serviceInstanceBingingResource;
        try{
            serviceInstanceBingingResource = ocdp.provisionResources(serviceInstanceId, bindingId);
        }catch (Exception e){
            logger.error("OCDP resource provision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            logger.info("Rollback LDAP user: " + accountName);
            try{
                this.removeLDAPUser(ldap, accountName);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            logger.info("Rollback kerberos principal: " + accountName);
            try{
                kc.removePrincipal(accountName +  "@ASIAINFO.COM");
            }catch(KerberosOperationException ex){
                ex.printStackTrace();
            }
            throw new OCDPServiceException("OCDP ressource provision fails due to: " + e.getLocalizedMessage());
        }
        // Set permission by Apache Ranger
        Map<String, Object> credentials = new HashMap<String, Object>();
        ArrayList<String> groupList = new ArrayList<String>(){{add("hadoop");}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> permList = new ArrayList<String>(){{add("read"); add("write"); add("execute");}};
        String policyName = UUID.randomUUID().toString();
        boolean policyCreateResult = false;
        int i = 0;
        while(i++ <= 20){
            logger.info("Try to create ranger policy...");
            policyCreateResult = ocdp.assignPermissionToResources(policyName, serviceInstanceBingingResource,
                    groupList, userList, permList);
            // TODO Need get a way to force sync up ldap users with ranger service, for temp solution will wait 60 sec
            if (! policyCreateResult){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else{
                logger.info("Ranger policy created.");
                // generate binding credentials
                credentials.put("serviceInstanceBingingUser", accountName);
                credentials.put("serviceInstanceBingingPwd", pwd);
                credentials.put("serviceInstanceBindingKeytab", keyTabString);
                credentials.put("serviceInstanceBingingResource", serviceInstanceBingingResource);
                credentials.put("rangerPolicyName", policyName);
                break;
            }
        }
        if (! policyCreateResult){
            logger.error("Ranger policy create fail.");
            logger.info("Rollback LDAP user: " + accountName);
            try{
                this.removeLDAPUser(ldap, accountName);
            }catch(Exception ex){
                ex.printStackTrace();
            }
            logger.info("Rollback kerberos principal: " + accountName);
            try{
                kc.removePrincipal(accountName +  "@ASIAINFO.COM");
            }catch(KerberosOperationException ex){
                ex.printStackTrace();
            }
            logger.info("Rollback OCDP resource: " + serviceInstanceBingingResource);
            try{
                ocdp.deprovisionResources(serviceInstanceBingingResource);
            }catch (Exception e){
                e.printStackTrace();
            }
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        binding = new ServiceInstanceBinding(bindingId, serviceInstanceId, credentials, null, request.getBoundAppGuid());

        bindingRepository.save(binding);

        return new CreateServiceInstanceAppBindingResponse().withCredentials(credentials);
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
            throws OCDPServiceException{
        String serviceId = request.getServiceInstanceId();
        String bindingId = request.getBindingId();
        ServiceInstanceBinding binding = getServiceInstanceBinding(serviceId, bindingId);

        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }

        Map<String, Object> credentials = binding.getCredentials();
        String accountName = (String)credentials.get("serviceInstanceBingingUser");
        String serviceInstanceBingingResource = (String)credentials.get("serviceInstanceBingingResource");
        String policyName = (String)credentials.get("rangerPolicyName");
        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // Unset permission by Apache Ranger
        boolean policyDeleteResult = ocdp.unassignPermissionFromResources(policyName);
        if(!policyDeleteResult)
        {
            logger.error("Ranger policy delete fail.");
            throw new OCDPServiceException("Ranger policy delete fail.");
        }
        // Delete kerberos principal for OCDP service instance binding
        logger.info("Delete service binding kerberos principal.");
        krbClient kc = new krbClient(this.clusterConfig);
        try{
            kc.removePrincipal(accountName +  "@ASIAINFO.COM");
        }catch(KerberosOperationException e){
            logger.error("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
        }
        // Delete LDAP user for OCDP service instance binding
        logger.info("Delete service binding ldap user.");
        LdapTemplate ldap = this.clusterConfig.getLdapTemplate();
        try{
            this.removeLDAPUser(ldap, accountName);
        }catch (Exception e){
            logger.error("Delete LDAP user fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete LDAP user fail due to: " + e.getLocalizedMessage());
        }
        // Delete Hadoop resource like hdfs folder, hbase table ...
        try{
            ocdp.deprovisionResources(serviceInstanceBingingResource);
        }catch(Exception e){
            logger.error("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
        }

        bindingRepository.delete(serviceId, bindingId);
    }

	protected ServiceInstanceBinding getServiceInstanceBinding(String serviceInstanceId, String bindingId) {
		return bindingRepository.findOne(serviceInstanceId, bindingId);
	}

    private void createLDAPUser(LdapTemplate ldap, String accountName, String groupName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        Attributes userAttributes = new BasicAttributes();
        userAttributes.put("memberOf", "cn=" + groupName + ",ou=Group,dc=asiainfo,dc=com");
        BasicAttribute classAttribute = new BasicAttribute("objectClass");
        classAttribute.add("account");
        userAttributes.put(classAttribute);
        ldap.bind(ldapName, null, userAttributes);
    }

    private void removeLDAPUser(LdapTemplate ldap, String accountName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("uid", accountName)
                .build();
        ldap.unbind(ldapName);
    }

}
