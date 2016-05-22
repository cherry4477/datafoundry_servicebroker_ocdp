package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;


import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.EtcdConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstanceBinding;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceBindingRepository;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Repository for ServiceInstanceBinding objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceBindingRepositoryImpl implements OCDPServiceInstanceBindingRepository {

    private com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceBindingRepositoryImpl(EtcdConfig etcdCfg){
        this.etcdClient = etcdCfg.getEtcdClient();
    }

    @Override
    public ServiceInstanceBinding findOne(String serviceInstanceId, String bindingId) {
        System.err.println("find one： OCDPServiceInstanceBinding");
        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId) == null){
            return null;
        }
        String id = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/id");
        String syslogDrainUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/syslogDrainUrl");
        String appGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/appGuid");
        String serviceInstanceUser = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/serviceInstanceUser");
        String serviceInstancePwd = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/serviceInstancePwd");
        String serviceInstanceResource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/Credentials/serviceInstanceResource");
        String rangerPolicyName = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyName");
        Map<String, Object> credentials = new HashMap<String, Object>() {
            {
                put("serviceInstanceUser", serviceInstanceUser);
                put("serviceInstancePwd", serviceInstancePwd);
                put("serviceInstanceResource", serviceInstanceResource);
                put("rangerPolicyName", rangerPolicyName);
            }
        };

        return new ServiceInstanceBinding(id, serviceInstanceId, credentials,syslogDrainUrl, appGuid);
    }

    @Override
    public void save(ServiceInstanceBinding binding) {
        String serviceInstanceId = binding.getServiceInstanceId();
        String bindingId = binding.getId();
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/id", binding.getId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/serviceInstanceId", binding.getServiceInstanceId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/syslogDrainUrl", binding.getSyslogDrainUrl());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/appGuid", binding.getAppGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/serviceInstanceUser",
                (String)binding.getCredentials().get("serviceInstanceUser"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/serviceInstancePwd",
                (String)binding.getCredentials().get("serviceInstancePwd"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/Credentials/serviceInstanceResource",
                (String)binding.getCredentials().get("serviceInstanceResource"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyName",
                (String)binding.getCredentials().get("rangerPolicyName"));
        System.err.println("save：OCDPServiceInstanceBinding");

    }

    @Override
    public void delete(String serviceInstanceId, String bindingId) {
        System.err.println("delete:OCDPServiceInstanceBinding");
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId);
    }

}