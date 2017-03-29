package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.lang.Thread;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.BrokerUtil;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;

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
public class OCDPServiceInstanceLifecycleService {

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceLifecycleService.class);

    @Autowired
	private OCDPServiceInstanceRepository repository;

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private LdapTemplate ldap;

    private krbClient kc;

    private etcdClient etcdClient;


    @Autowired
    public OCDPServiceInstanceLifecycleService(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.ldap = clusterConfig.getLdapTemplate();
        this.kc = new krbClient(clusterConfig);
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Async
    public Future<CreateServiceInstanceResponse> doCreateServiceInstanceAsync(CreateServiceInstanceRequest request) throws OCDPServiceException {
        return new AsyncResult<CreateServiceInstanceResponse>(
                doCreateServiceInstance(request)
        );
    }

	public CreateServiceInstanceResponse doCreateServiceInstance(CreateServiceInstanceRequest request) throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        String planId = request.getPlanId();
        ServiceInstance instance = new ServiceInstance(request);

        String accountName = request.getSpaceGuid();
        String ldapGroupName = this.clusterConfig.getLdapGroup();
        String ldapGroupId = this.clusterConfig.getLdapGroupId();
        String krbRealm = this.clusterConfig.getKrbRealm();
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        instance.setDashboardUrl(ocdp.getDashboardUrl());

        //Create LDAP user and related kerberos principal/keytab if it not exists
        boolean newCreatedLDAPUser = false;
        try{
            if(! BrokerUtil.isLDAPUserExist(this.ldap, accountName)){
                logger.info("create new ldap user.");
                newCreatedLDAPUser = true;
                BrokerUtil.createLDAPUser(this.ldap, this.etcdClient, accountName, ldapGroupName, ldapGroupId);
            }
        }catch (Exception e){
            logger.error("LDAP user create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("LDAP user create fail due to: " + e.getLocalizedMessage());
        }

        //Create new kerberos principal/keytab for new LDAP user
        String pn = accountName + "@" + krbRealm;
        String pwd;
        String keyTabString = "";
        if (newCreatedLDAPUser){
            logger.info("create new kerberos principal.");
            // Generate krb password and store it to etcd
            pwd = UUID.randomUUID().toString();
            etcdClient.write("/servicebroker/ocdp/user/krb/" + pn, pwd);
            try{
                this.kc.createPrincipal(pn, pwd);
                keyTabString = this.kc.createKeyTabString(pn, pwd, null);
            }catch(KerberosOperationException e){
                logger.error("Kerberos principal create fail due to: " + e.getLocalizedMessage());
                e.printStackTrace();
                logger.info("Rollback LDAP user: " + accountName);
                try{
                    BrokerUtil.removeLDAPUser(this.ldap, accountName);
                }catch (Exception ex){
                    ex.printStackTrace();
                }
                throw new OCDPServiceException("Kerberos principal create fail due to: " + e.getLocalizedMessage());
            }
        } else {
            // Get krb principal's password from etcd
            pwd = etcdClient.readToString("/servicebroker/ocdp/user/krb/" + pn);
        }

        // Create Hadoop resource like hdfs folder, hbase table ...
        String serviceInstanceResource;
        try{
            serviceInstanceResource = ocdp.provisionResources(serviceDefinitionId, planId, serviceInstanceId, null, accountName);
        }catch (Exception e){
            logger.error("OCDP resource provision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            if(newCreatedLDAPUser){
                rollbackLDAPAndKrb(accountName, pn);
            }
            throw new OCDPServiceException("OCDP ressource provision fails due to: " + e.getLocalizedMessage());
        }

        // Set permission by Apache Ranger
        Map<String, String> credentials = new HashMap<String, String>();
        String policyName = UUID.randomUUID().toString();
        String policyId = null;
        int i = 0;
        logger.info("Try to create ranger policy...");
        while(i++ <= 40){
            policyId = ocdp.assignPermissionToResources(policyName, new ArrayList<String>(){{add(serviceInstanceResource);}}, accountName, ldapGroupName);
            // TODO Need get a way to force sync up ldap users with ranger service, for temp solution will wait 60 sec
            if (policyId == null){
                try{
                    Thread.sleep(3000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }else{
                logger.info("Ranger policy created.");
                credentials.put("username", pn);
                credentials.put("password", pwd);
                credentials.put("keytab", keyTabString);
                credentials.put("name", serviceInstanceResource);
                credentials.put("rangerPolicyId", policyId);
                break;
            }
        }
        if (policyId == null){
            logger.error("Ranger policy create fail.");
            if(newCreatedLDAPUser){
                rollbackLDAPAndKrb(accountName, pn);
            }
            logger.info("Rollback OCDP resource: " + serviceInstanceResource);
            try{
                ocdp.deprovisionResources(serviceInstanceResource);
            }catch (Exception e){
                e.printStackTrace();
            }
            throw new OCDPServiceException("Ranger policy create fail.");
        }
        // Save service instance
        instance.setCredential(credentials);
        repository.save(instance);

        /**
        CreateServiceInstanceResponse response = new CreateServiceInstanceResponse()
                .withDashboardUrl(instance.getDashboardUrl())
                .withAsync(false);
         Do not return OCDP components dashboard to DF, due to current Hadoop component dashboard not support multi-tenant function.
         For details, please refer to https://github.com/asiainfoLDP/datafoundry_servicebroker_ocdp/issues/2
        **/

        CreateServiceInstanceResponse response = new CreateServiceInstanceResponse().withAsync(false);
        return response;
	}

    @Async
    public Future<DeleteServiceInstanceResponse> doDeleteServiceInstanceAsync(DeleteServiceInstanceRequest request, ServiceInstance instance)
            throws OCDPServiceException {
        return new AsyncResult<DeleteServiceInstanceResponse>(doDeleteServiceInstance(request, instance));
    }

    public DeleteServiceInstanceResponse doDeleteServiceInstance(DeleteServiceInstanceRequest request, ServiceInstance instance)
            throws OCDPServiceException {
        String serviceDefinitionId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();

        Map<String, String> Credential = instance.getServiceInstanceCredentials();
        String serviceInstanceResource = Credential.get("name");
        String policyId = Credential.get("rangerPolicyId");
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);

        // Unset permission by Apache Ranger
        boolean policyDeleteResult = ocdp.unassignPermissionFromResources(policyId);
        if(!policyDeleteResult)
        {
            logger.error("Ranger policy delete fail.");
            throw new OCDPServiceException("Ranger policy delete fail.");
        }

        // Delete Hadoop resource like hdfs folder, hbase table ...
        try{
            ocdp.deprovisionResources(serviceInstanceResource);
        }catch (Exception e){
            logger.error("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new OCDPServiceException("OCDP resource deprovision fail due to: " + e.getLocalizedMessage());
        }

        repository.delete(serviceInstanceId);

		return new DeleteServiceInstanceResponse().withAsync(false);
	}

    public String getOCDPServiceDashboard(String serviceDefinitionId){
        OCDPAdminService ocdp = getOCDPAdminService(serviceDefinitionId);
        return ocdp.getDashboardUrl();
    }

    private OCDPAdminService getOCDPAdminService(String serviceDefinitionId){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceDefinitionId)
        );
    }

    private void rollbackLDAPAndKrb(String accountName, String pn){
        logger.info("Rollback LDAP user: " + accountName);
        try{
            BrokerUtil.removeLDAPUser(this.ldap, accountName);
        }catch (Exception ex){
            logger.error("Delete LDAP user fail due to: " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
        logger.info("Rollback kerberos principal: " + accountName);
        try{
            this.kc.removePrincipal(pn);
        }catch(KerberosOperationException ex){
            logger.error("Delete kerbreos principal fail due to: " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }

}