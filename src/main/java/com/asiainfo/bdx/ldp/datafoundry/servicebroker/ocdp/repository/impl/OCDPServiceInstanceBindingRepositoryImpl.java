package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;


import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;

/**
 * Implementation of Repository for ServiceInstanceBinding objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceBindingRepositoryImpl implements OCDPServiceInstanceBindingRepository {

    @Override
    public ServiceInstanceBinding findOne(String bindingId) {
        System.err.println("find one： MysqlServiceInstanceBinding");
        return null;
    }

    @Override
    public void save(ServiceInstanceBinding binding) {
        System.err.println("save：MysqlServiceInstanceBinding");
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(String bindingId) {
        System.err.println("delete:MysqlServiceInstanceBinding");
        // TODO Auto-generated method stub

    }

}