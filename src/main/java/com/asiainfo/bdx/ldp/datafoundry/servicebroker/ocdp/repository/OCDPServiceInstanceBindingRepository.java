package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;

/**
 * Repository for ServiceInstanceBinding objects
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPServiceInstanceBindingRepository {

    ServiceInstanceBinding findOne(String serviceInstanceId, String bindingId);

    void save(ServiceInstanceBinding binding);

    void delete(String serviceInstanceId, String bindingId);

}
