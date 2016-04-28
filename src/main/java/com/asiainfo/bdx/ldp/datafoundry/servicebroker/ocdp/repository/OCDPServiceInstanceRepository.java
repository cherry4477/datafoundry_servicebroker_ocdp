package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;

/**
 * Repository for ServiceInstance objects
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPServiceInstanceRepository {

    ServiceInstance findOne(String serviceInstanceId);

    void save(ServiceInstance instance);

    void delete(String serviceInstanceId);

}