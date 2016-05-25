package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.io.IOException;
import java.lang.Thread;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;


import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationRequest;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationResponse;
import org.springframework.cloud.servicebroker.model.OperationState;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.krbConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ldapConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

import org.springframework.cloud.servicebroker.service.ServiceInstanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;
import org.springframework.ldap.support.LdapNameBuilder;

/**
 * OCDP impl to manage hadoop service instances.  Creating a service does the following:
 * creates a new service instance user,
 * create hadoop service instance(e.g. hdfs dir, hbase table...),
 * set permission for service instance user,
 * saves the ServiceInstance info to the hdaoop repository.
 *  
 * @author whitebai1986@gmail.com
 */
@Service
public class OCDPServiceInstanceService implements ServiceInstanceService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceService.class);

    @Autowired
	private OCDPServiceInstanceRepository repository;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ldapConfig ldapConfig;

    @Autowired
    public krbConfig krbConfig;

    public OCDPServiceInstanceService() {}

    private OCDPAdminService getOCDPAdminService(String serviceID){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceID)
        );
    }

	@Override
	public CreateServiceInstanceResponse createServiceInstance(CreateServiceInstanceRequest request) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance != null) {
            throw new ServiceInstanceExistsException(request.getServiceInstanceId(), request.getServiceDefinitionId());
        }
        instance = new ServiceInstance(request);

        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // Create LDAP user for service instance
        logger.info("create ldap user.");
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        String accountName = "serviceInstance_" + UUID.randomUUID().toString();
        try{
            this.createLDAPUser(ldap, accountName);
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }

        //Create Kerberos principal for new LDAP user
        logger.info("create kerberos principal.");
        krbClient kc = new krbClient(this.krbConfig);
        String pn = accountName +  "@ASIAINFO.COM";
        String pwd = UUID.randomUUID().toString();
        try{
            kc.createPrincipal(pn, pwd);
        }catch(KerberosOperationException e){
            logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            logger.info("Rollback LDAP user: " + accountName);
            this.removeLDAPUser(ldap, accountName);
            throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
        }

        // Create Hadoop resource like hdfs folder, hbase table ...
        String serviceInstanceResource;
        try{
            serviceInstanceResource = ocdp.provisionResources(serviceInstanceId, null);
        }catch (IOException e){
            logger.error("OCDP resource provision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            logger.info("Rollback LDAP user: " + accountName);
            this.removeLDAPUser(ldap, accountName);
            logger.info("Rollback kerberos principal: " + accountName);
            try{
                kc.removePrincipal(accountName +  "@ASIAINFO.COM");
            }catch(KerberosOperationException ex){
                ex.printStackTrace();
            }
            throw new OCDPServiceException("OCDP ressource provision fails due to: " + e.getLocalizedMessage());
        }

        // Set permission by Apache Ranger
        Map<String, String> credentials = new HashMap<String, String>();
        ArrayList<String> groupList = new ArrayList<String>(){{add("hadoop");}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> permList = new ArrayList<String>(){{add("read"); add("write"); add("execute");}};
        String policyName = UUID.randomUUID().toString();
        boolean policyCreateResult = false;
        int i = 0;
        while(i++ <= 20){
            logger.info("Try to create ranger policy...");
            policyCreateResult = ocdp.assignPermissionToResources(policyName, serviceInstanceResource,
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
                credentials.put("serviceInstanceUser", accountName);
                credentials.put("serviceInstanceResource", serviceInstanceResource);
                credentials.put("rangerPolicyName", policyName);
                break;
            }
        }
        if (! policyCreateResult){
            logger.error("Ranger policy create fail.");
            logger.info("Rollback LDAP user: " + accountName);
            this.removeLDAPUser(ldap, accountName);
            logger.info("Rollback kerberos principal: " + accountName);
            try{
                kc.removePrincipal(accountName +  "@ASIAINFO.COM");
            }catch(KerberosOperationException ex){
                ex.printStackTrace();
            }
            logger.info("Rollback OCDP resource: " + serviceInstanceResource);
            try{
                ocdp.deprovisionResources(serviceInstanceResource);
            }catch (IOException e){
                e.printStackTrace();
            }
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        instance.setCredential(credentials);

        repository.save(instance);

		return new CreateServiceInstanceResponse();
	}

	@Override
	public GetLastServiceOperationResponse getLastOperation(GetLastServiceOperationRequest request) {
		return new GetLastServiceOperationResponse().withOperationState(OperationState.SUCCEEDED);
	}

	public ServiceInstance getServiceInstance(String id) {
		return repository.findOne(id);
	}

	@Override
	public DeleteServiceInstanceResponse deleteServiceInstance(DeleteServiceInstanceRequest request)
            throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(serviceInstanceId);
        }
        Map<String, String> Credential = instance.getServiceInstanceMetadata();
        String accountName = Credential.get("serviceInstanceUser");
        String serviceInstanceResource = Credential.get("serviceInstanceResource");
        String policyName = Credential.get("rangerPolicyName");
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        // Unset permission by Apache Ranger
        boolean policyDeleteResult = ocdp.unassignPermissionFromResources(policyName);
        if(!policyDeleteResult)
        {
            logger.error("Ranger policy delete fail.");
            throw new OCDPServiceException("Ranger policy delete fail.");
        }
        // Delete Kerberos principal for new LDAP user
        logger.info("Delete kerberos principal.");
        krbClient kc = new krbClient(this.krbConfig);
        try{
            kc.removePrincipal(accountName +  "@ASIAINFO.COM");
        }catch(KerberosOperationException e){
            logger.error("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete kerbreos principal fail due to: " + e.getLocalizedMessage());
        }
        // Delete LDAP user for service instance
        logger.info("Delete ldap user.");
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        try{
            this.removeLDAPUser(ldap, accountName);
        }catch (Exception e){
            logger.error("Delete LDAP user fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("Delete LDAP user fail due to: " + e.getLocalizedMessage());
        }
        // Delete Hadoop resource like hdfs folder, hbase table ...
        try{
            ocdp.deprovisionResources(serviceInstanceResource);
        }catch (IOException e){
            logger.error("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
        }

        repository.delete(serviceInstanceId);

		return new DeleteServiceInstanceResponse();
	}

	@Override
	public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request) {
        // TODO OCDP service instance update
        return new UpdateServiceInstanceResponse();
	}

    private void createLDAPUser(LdapTemplate ldap, String accountName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("cn", accountName)
                .build();
        Attributes userAttributes = new BasicAttributes();
        userAttributes.put("sn", accountName);
        BasicAttribute classAttribute = new BasicAttribute("objectClass");
        classAttribute.add("top");
        classAttribute.add("person");
        userAttributes.put(classAttribute);
        ldap.bind(ldapName, null, userAttributes);
    }

    private void removeLDAPUser(LdapTemplate ldap, String accountName){
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("cn", accountName)
                .build();
        ldap.unbind(ldapName);
    }
}