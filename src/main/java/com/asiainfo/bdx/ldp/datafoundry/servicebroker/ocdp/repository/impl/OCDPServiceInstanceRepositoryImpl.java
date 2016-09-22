package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceRepositoryImpl.class);

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceRepositoryImpl(ClusterConfig clusterConfig){
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public ServiceInstance findOne(String serviceInstanceId) {
        logger.info("find one OCDPServiceInstance: " + serviceInstanceId);

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
        String username = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/username");
        String resource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/name");
        String rangerPolicyId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId +
                "/Credentials/rangerPolicyId");
        Map<String, String> Credential = new HashMap<String, String>() {
            {
                put("username", username);
                put("name", resource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
        instance.setCredential(Credential);

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
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/username",
                instance.getServiceInstanceCredentials().get("username"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/name",
                instance.getServiceInstanceCredentials().get("name"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/Credentials/rangerPolicyId",
                instance.getServiceInstanceCredentials().get("rangerPolicyId"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/dashboardUrl",
                instance.getDashboardUrl());
        etcdClient.createDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings");
        System.err.println("save OCDPServiceInstance: " + serviceInstanceId);
    }

    @Override
    public void delete(String serviceInstanceId) {
        logger.info("delete OCDPServiceInstance: " + serviceInstanceId );
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId, true);
    }

}