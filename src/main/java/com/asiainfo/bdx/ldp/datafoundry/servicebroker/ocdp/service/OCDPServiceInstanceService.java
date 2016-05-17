package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.ArrayList;
import java.util.UUID;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.ldap.LdapName;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.ShellCommandUtil;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationRequest;
import org.springframework.cloud.servicebroker.model.GetLastServiceOperationResponse;
import org.springframework.cloud.servicebroker.model.OperationState;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.UpdateServiceInstanceResponse;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.krbClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.krbConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ldapConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.rangerConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.*;

import org.springframework.cloud.servicebroker.service.ServiceInstanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Service;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.support.LdapNameBuilder;
import static org.springframework.ldap.query.LdapQueryBuilder.query;

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

	private OCDPAdminService ocdp;
	
	private OCDPServiceInstanceRepository repository;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ldapConfig ldapConfig;

    @Autowired
    public krbConfig krbConfig;

    @Autowired
    public rangerConfig rangerConfig;

	@Autowired
	public OCDPServiceInstanceService(OCDPAdminService ocdp, OCDPServiceInstanceRepository repository) {
		this.ocdp = ocdp;
		this.repository = repository;
	}
	
	@Override
	public CreateServiceInstanceResponse createServiceInstance(CreateServiceInstanceRequest request) {
		// TODO OCDP service instance create
        System.out.println("Provison service instance.");
        /**
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        //LdapQuery query = query().base("ou=People");
        //List<String> list = ldap.list(query.base());

        String accountName = "servIns_" + UUID.randomUUID().toString();
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
        krbClient kc = new krbClient(this.krbConfig);
        try{
            String pn = accountName +  "/admin@ASIAINFO.COM";
            kc.createPrincipal(pn, "111111");
            ShellCommandUtil.Result re = kc.invokeKAdmin("listprincs");
            System.out.println(re.getStdout());
            Keytab kt = kc.createKeyTab(pn, "111111", null);
            System.out.println(kt);
            kc.createKetTabFile(kt, "/tmp/test.keytab");
        }catch(KerberosOperationException e){
            e.printStackTrace();
        }
         **/
        rangerClient rc = rangerConfig.getRangerClient();
        //System.out.println(rc.getPolicy("1"));
        ArrayList<String> groupList = new ArrayList<String>(){{
            add("public");
        }};
        ArrayList<String> userList = new ArrayList<String>(){{
            add("hdfs");
        }};
        ArrayList<String> permList = new ArrayList<String>(){{
            add("read");
        }};
        //System.out.println(rc.getPolicy("1"));
        rc.createPolicy("testpolicy", "/user/test", "desc",
                "OCDP_hadoop", "hdfs", groupList, userList, permList);
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
		// TODO OCDP service instance delete
        System.out.println("Deprovison service instance.");
        /**
        LdapTemplate ldap = this.ldapConfig.getLdapTemplate();
        String baseDN = "ou=People";
        LdapName ldapName = LdapNameBuilder.newInstance(baseDN)
                .add("cn", "servIns_499bb289-0c3d-4de6-9ad9-9fa0617f0e85")
                .build();
        ldap.unbind(ldapName);
        krbClient kc = new krbClient(this.krbConfig);
        try{
            kc.removePrincipal("baikai/admin@ASIAINFO.COM");
        }catch (KerberosOperationException e){
            e.printStackTrace();
        }
         **/
        rangerClient rc = rangerConfig.getRangerClient();
        System.out.println(rc.removePolicy("24"));
		return new DeleteServiceInstanceResponse();
	}

	@Override
	public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request) {
        // TODO OCDP service instance update
        return new UpdateServiceInstanceResponse();
	}

}