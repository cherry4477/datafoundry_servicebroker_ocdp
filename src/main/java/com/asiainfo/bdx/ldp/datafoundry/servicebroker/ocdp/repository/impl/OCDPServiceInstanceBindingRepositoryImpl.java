package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.repository.impl;


import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger = LoggerFactory.getLogger(OCDPServiceInstanceBindingRepositoryImpl.class);

    private etcdClient etcdClient;

    @Autowired
    public OCDPServiceInstanceBindingRepositoryImpl(ClusterConfig clusterConfig){
        this.etcdClient = clusterConfig.getEtcdClient();
    }

    @Override
    public ServiceInstanceBinding findOne(String serviceInstanceId, String bindingId) {
        logger.info("find one OCDPServiceInstanceBinding: " + bindingId);
        if(etcdClient.read("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId) == null){
            return null;
        }
        String id = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/id");
        String syslogDrainUrl = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/syslogDrainUrl");
        String appGuid = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/appGuid");
        String planId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId +"/planId");
        String uri = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/uri");
        String username = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/username");
        String password = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/password");
        String keytab = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/keytab");
        String host = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/host");
        String port = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/port");
        String resource = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/name");
        String rangerPolicyId = etcdClient.readToString("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyId");
        Map<String, Object> credentials = new HashMap<String, Object>() {
            {
                put("uri", uri);
                put("username", username);
                put("password", password);
                put("keytab", keytab);
                put("host", host);
                put("port", port);
                put("name", resource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };

        return new ServiceInstanceBinding(id, serviceInstanceId, credentials,syslogDrainUrl, appGuid, planId);
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
                bindingId + "/planId", binding.getPlanId());
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/uri",
                (String)binding.getCredentials().get("uri"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/username",
                (String)binding.getCredentials().get("username"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/password",
                (String)binding.getCredentials().get("password"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/keytab",
                (String)binding.getCredentials().get("keytab"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/host",
                (String)binding.getCredentials().get("host"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/port",
                (String)binding.getCredentials().get("port"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                        bindingId + "/Credentials/name",
                (String)binding.getCredentials().get("name"));
        etcdClient.write("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" +
                bindingId + "/Credentials/rangerPolicyId",
                (String)binding.getCredentials().get("rangerPolicyId"));
        System.err.println("save：OCDPServiceInstanceBinding");

    }

    @Override
    public void delete(String serviceInstanceId, String bindingId) {
        System.err.println("delete:OCDPServiceInstanceBinding");
        logger.info("delete:OCDPServiceInstanceBinding: " + bindingId);
        etcdClient.deleteDir("/servicebroker/ocdp/instance/" + serviceInstanceId + "/bindings/" + bindingId, true);
    }

}