package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.EtcdConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.ServiceInstance;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.OCDPServiceInstanceRepository;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Repository for ServiceInstance objects
 *
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPServiceInstanceRepositoryImpl implements OCDPServiceInstanceRepository {

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceRepositoryImpl(EtcdConfig etcdCfg){
        this.etcdClient = etcdCfg.getEtcdClient();
    }

    @Override
    public ServiceInstance findOne(String serviceInstanceId) {
        System.err.println("find one OCDPServiceInstance : " + serviceInstanceId);
        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId) == null){
            return null;
        }
        String orgGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid");
        String spaceGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid");
        String instanceId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id");
        String planId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId");
        String dashboardUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl");
        ServiceInstance instance = new ServiceInstance(serviceInstanceId, instanceId, planId, orgGuid, spaceGuid,
                dashboardUrl);
        String serviceInstanceUser = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/metadata/serviceInstanceUser");
        String serviceInstanceResource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/metadata/serviceInstanceResource");
        String rangerPolicyName = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/metadata/rangerPolicyName");
        Map<String, String> serviceInstanceMatadata = new HashMap<String, String>() {
            {
                put("serviceInstanceUser", serviceInstanceUser);
                put("serviceInstanceResource", serviceInstanceResource);
                put("rangerPolicyName", rangerPolicyName);
            }
        };
        instance.setServiceInstanceMetadata(serviceInstanceMatadata);

        return instance;
    }

    @Override
    public void save(ServiceInstance instance) {
        String serviceInstanceId = instance.getServiceInstanceId();
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/organizationGuid",
                instance.getOrganizationGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/spaceGuid",
                instance.getSpaceGuid());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/id",
                instance.getServiceInstanceId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/planId",
                instance.getPlanId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/metadata/serviceInstanceUser",
                instance.getServiceInstanceMetadata().get("serviceInstanceUser"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/metadata/serviceInstanceResource",
                instance.getServiceInstanceMetadata().get("serviceInstanceResource"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/metadata/rangerPolicyName",
                instance.getServiceInstanceMetadata().get("rangerPolicyName"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl",
                instance.getDashboardUrl());
        etcdClient.createDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings");
        System.err.println("save OCDPServiceInstance: " + serviceInstanceId);
    }

    @Override
    public void delete(String serviceInstanceId) {
        System.err.println("delete OCDPServiceInstance: " + serviceInstanceId);
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId);
    }

}