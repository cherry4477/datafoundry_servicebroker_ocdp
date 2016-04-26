package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Repository for ServiceInstance objects
 * 
 * @author sgreenberg@pivotal.io
 *
 */
public interface MongoServiceInstanceRepository extends MongoRepository<ServiceInstance, String> {

}