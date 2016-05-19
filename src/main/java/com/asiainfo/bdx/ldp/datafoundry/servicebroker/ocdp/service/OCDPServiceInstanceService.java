package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
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
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
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
        ServiceInstance instance = repository.findOne(request.getServiceInstanceId());
        if (instance != null) {
            throw new ServiceInstanceExistsException(request.getServiceInstanceId(), request.getServiceDefinitionId());
        }
        instance = new ServiceInstance(request);

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);

        // TODO Create LDAP user for service instance
        System.out.println("create ldap user.");
        // TODO Create Kerberos principal for new LDAP user
        System.out.println("create kerberos principal.");
        // TODO Create Hadoop resource like hdfs folder, hbase table ...
        ocdp.provisionResources();
        // TODO Set permission by Apache Ranger
        ocdp.assignPermissionToResources();

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

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // TODO Delete LDAP user for service instance
        System.out.println("Delete ldap user.");
        // TODO Delete Kerberos principal for new LDAP user
        System.out.println("Delete kerberos principal.");
        // TODO Delete Hadoop resource like hdfs folder, hbase table ...
        ocdp.deprovisionResources();
        // TODO Unset permission by Apache Ranger
        ocdp.unassignPermissionFromResources();

        repository.delete(instanceId);

		return new DeleteServiceInstanceResponse();
	}

	@Override
	public UpdateServiceInstanceResponse updateServiceInstance(UpdateServiceInstanceRequest request) {
        // TODO OCDP service instance update
        return new UpdateServiceInstanceResponse();
	}

}