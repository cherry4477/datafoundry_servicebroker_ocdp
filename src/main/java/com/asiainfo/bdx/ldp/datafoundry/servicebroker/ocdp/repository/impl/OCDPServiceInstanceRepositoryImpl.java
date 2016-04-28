package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;

import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;

/**
 * Implementation of Repository for ServiceInstance objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceRepositoryImpl implements OCDPServiceInstanceRepository {

    @Override
    public ServiceInstance findOne(String serviceInstanceId) {
        // TODO Auto-generated method stub
        System.err.println("find one:MysqlServiceInstance");
        return null;
    }

    @Override
    public void save(ServiceInstance instance) {
        System.err.println("save：MysqlServiceInstance");
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(String serviceInstanceId) {
        System.err.println("delete：MysqlServiceInstance");
        // TODO Auto-generated method stub
    }

}