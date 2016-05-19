package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.Map;
import java.util.Collections;

import org.springframework.cloud.servicebroker.model.CreateServiceInstanceAppBindingResponse;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceBindingResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceBindingRequest;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;
import org.springframework.cloud.servicebroker.service.ServiceInstanceBindingService;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceBindingDoesNotExistException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

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
        // TODO Create kerberos principal for OCDP service instance binding
        System.out.println("create service binding kerberos principal.");
        // TODO Create Hadoop resource like hdfs folder, hbase table ...
        ocdp.provisionResources();
        // TODO Set permission by Apache Ranger
        ocdp.assignPermissionToResources();
        //TODO generate binding credentials
        Map<String, Object> credentials = ocdp.generateCredentials();

        binding = new ServiceInstanceBinding(bindingId, serviceInstanceId, credentials, null, request.getBoundAppGuid());
        bindingRepository.save(binding);

        return new CreateServiceInstanceAppBindingResponse();
	}

	@Override
	public void deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request) {
        String serviceId = request.getServiceDefinitionId();
        String bindingId = request.getBindingId();
        ServiceInstanceBinding binding = getServiceInstanceBinding(bindingId);

        if (binding == null) {
            throw new ServiceInstanceBindingDoesNotExistException(bindingId);
        }

        OCDPAdminService ocdp = getOCDPAdminService(serviceId);
        // TODO Delete LDAP user for OCDP service instance binding
        System.out.println("Delete service binding ldap user.");
        // TODO Delete kerberos principal for OCDP service instance binding
        System.out.println("Delete service binding kerberos principal.");
        // TODO Delete Hadoop resource like hdfs folder, hbase table ...
        ocdp.deprovisionResources();
        // TODO Unset permission by Apache Ranger
        ocdp.unassignPermissionFromResources();

        bindingRepository.delete(bindingId);
    }

	protected ServiceInstanceBinding getServiceInstanceBinding(String id) {
		return bindingRepository.findOne(id);
	}

}
