package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.Map;
import java.util.Collections;
import java.util.UUID;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ldapConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.krbConfig;

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

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;

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

    @Autowired
	private OCDPServiceInstanceBindingRepository bindingRepository;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ldapConfig ldapConfig;

    @Autowired
    public krbConfig krbConfig;

    public OCDPServiceInstanceBindingService() {}

    private OCDPAdminService getOCDPAdminService(String serviceID){
        return  (OCDPAdminService) this.context.getBean(
                OCDPAdminServiceMapper.getOCDPAdminService(serviceID)
        );
    }

	@Override
	public CreateServiceInstanceBindingResponse createServiceInstanceBinding(CreateServiceInstanceBindingRequest request) {
        String serviceId = request.getServiceDefinitionId();
		String bindingId = request.getBindingId();
		String serviceInstanceId = request.getServiceInstanceId();
        ServiceInstanceBinding binding = bindingRepository.findOne(bindingId);
        if (binding != null) {
            throw new ServiceInstanceBindingExistsException(serviceInstanceId, bindingId);
        }

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // TODO Create LDAP user for OCDP service instance binding
        System.out.println("create service binding ldap user.");
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        String accountName = "serviceBinding_" + UUID.randomUUID().toString();
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

        // Create kerberos principal for OCDP service instance binding
        System.out.println("create service binding kerberos principal.");
        krbClient kc = new krbClient(this.krbConfig);
        try{
            String pn = accountName +  "@ASIAINFO.COM";
            String pwd = UUID.randomUUID().toString();
            kc.createPrincipal(pn, pwd);
            //Keytab kt = kc.createKeyTab(pn, pwd, null);
        }catch(KerberosOperationException e){
            e.printStackTrace();
        }
        // TODO Create Hadoop resource like hdfs folder, hbase table ...
        ocdp.provisionResources(serviceId);
        // TODO Set permission by Apache Ranger
        //ocdp.assignPermissionToResources();
        //TODO generate binding credentials
        Map<String, Object> credentials = ocdp.generateCredentials();

        binding = new ServiceInstanceBinding(bindingId, serviceInstanceId, credentials, null, request.getBoundAppGuid());
        bindingRepository.save(binding);

        return new CreateServiceInstanceAppBindingResponse();
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request) {
        String serviceId = request.getServiceDefinitionId();
        String instanceId = request.getServiceInstanceId();
        String bindingId = request.getBindingId();
        ServiceInstanceBinding binding = getServiceInstanceBinding(bindingId);

        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // TODO Unset permission by Apache Ranger
        //ocdp.unassignPermissionFromResources();
        // TODO Delete kerberos principal for OCDP service instance binding
        System.out.println("Delete service binding kerberos principal.");
        // TODO Delete LDAP user for OCDP service instance binding
        System.out.println("Delete service binding ldap user.");
        // TODO Delete Hadoop resource like hdfs folder, hbase table ...
        ocdp.deprovisionResources(instanceId);

        bindingRepository.delete(bindingId);
    }

	protected ServiceInstanceBinding getServiceInstanceBinding(String id) {
		return bindingRepository.findOne(id);
	}

}
