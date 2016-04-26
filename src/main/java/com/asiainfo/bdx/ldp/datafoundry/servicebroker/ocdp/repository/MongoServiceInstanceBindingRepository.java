package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Repository for ServiceInstanceBinding objects
 * 
 * @author sgreenberg@pivotal.io
 *
 */
public interface MongoServiceInstanceBindingRepository extends MongoRepository<ServiceInstanceBinding, String> {

}
