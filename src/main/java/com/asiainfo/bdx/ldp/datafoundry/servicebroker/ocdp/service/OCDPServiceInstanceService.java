package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.io.IOException;
import java.net.URI;
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
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.krbConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ldapConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;

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
	public CreateServiceInstanceResponse createServiceInstance(CreateServiceInstanceRequest request) {
        String serviceId = request.getServiceDefinitionId();
        String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstance instance = repository.findOne(serviceInstanceId);
        if (instance != null) {
            throw new ServiceInstanceExistsException(request.getServiceInstanceId(), request.getServiceDefinitionId());
        }
        instance = new ServiceInstance(request);

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);

        // Create LDAP user for service instance
        System.out.println("create ldap user.");
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        String accountName = "serviceInstance_" + UUID.randomUUID().toString();
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

        //Create Kerberos principal for new LDAP user
        System.out.println("create kerberos principal.");
        krbClient kc = new krbClient(this.krbConfig);
        try{
            String pn = accountName +  "@ASIAINFO.COM";
            String pwd = UUID.randomUUID().toString();
            kc.createPrincipal(pn, pwd);
            //Keytab kt = kc.createKeyTab(pn, pwd, null);
        }catch(KerberosOperationException e){
            e.printStackTrace();
        }

        // Create Hadoop resource like hdfs folder, hbase table ...
        String serviceInstanceResource = ocdp.provisionResources(serviceInstanceId);

        // Set permission by Apache Ranger
        ArrayList<String> groupList = new ArrayList<String>(){{add("hadoop");}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> permList = new ArrayList<String>(){{add("read"); add("write"); add("execute");}};
        String policyName = UUID.randomUUID().toString();
        String rangerPolicyName = ocdp.assignPermissionToResources(policyName, serviceInstanceResource,
                groupList, userList, permList);

        Map<String, String> serviceInstanceMatadata = new HashMap<String, String>() {
            {
                put("serviceInstanceUser", accountName);
                put("serviceInstanceResource", serviceInstanceResource);
                put("rangerPolicyName", rangerPolicyName);
            }
        };
        instance.setServiceInstanceMetadata(serviceInstanceMatadata);

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
	public DeleteServiceInstanceResponse deleteServiceInstance(DeleteServiceInstanceRequest request) throws OCDPServiceException {
        String serviceId = request.getServiceDefinitionId();
        String instanceId = request.getServiceInstanceId();
        ServiceInstance instance = repository.findOne(instanceId);
        if (instance == null) {
            throw new ServiceInstanceDoesNotExistException(instanceId);
        }
        Map<String, String> serviceInstanceMatadata = instance.getServiceInstanceMetadata();
        String accountName = serviceInstanceMatadata.get("accountName");
        String serviceInstanceResource = serviceInstanceMatadata.get("serviceInstanceResource");
        String policyName = serviceInstanceMatadata.get("rangerPolicyName");
        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // Unset permission by Apache Ranger
        ocdp.unassignPermissionFromResources(policyName);
        // Delete Kerberos principal for new LDAP user
        System.out.println("Delete kerberos principal.");
        krbClient kc = new krbClient(this.krbConfig);
        try{
            String pn = accountName +  "@ASIAINFO.COM";
            kc.removePrincipal(pn);
        }catch(KerberosOperationException e){
            e.printStackTrace();
        }
        // Delete LDAP user for service instance
        System.out.println("Delete ldap user.");
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("cn", accountName)
                .build();
        ldap.unbind(ldapName);
        // Delete Hadoop resource like hdfs folder, hbase table ...
        ocdp.deprovisionResources(serviceInstanceResource);

        repository.delete(instanceId);

		return new DeleteServiceInstanceResponse();
	}

	@Override
	public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request) {
        // TODO OCDP service instance update
        return new UpdateServiceInstanceResponse();
	}

}