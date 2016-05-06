package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.util.*;

import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Autowired;

import com.justinsb.etcd.EtcdResult;
import com.google.gson.*;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

@Configuration
public class CatalogConfig {

    @Autowired
    private EtcdConfig etcdCfg;

    static final Gson gson = new GsonBuilder().create();

    @Bean
    public Catalog catalog() {
        return new Catalog(
                this.getServiceDefinitions()
        );
    }

    private List<ServiceDefinition> getServiceDefinitions() {
        ArrayList<ServiceDefinition> serviceDefinitions = new ArrayList<ServiceDefinition>();
        etcdClient etcdClient = etcdCfg.getEtcdClient();
        EtcdResult result = etcdClient.read("/servicebroker/ocdp/catalog");
        if (result.node != null) {
            Map<String, Object> sdMetadata = new HashMap<>();
            String catalogString = result.node.value;
            Catalog catalogObj = gson.fromJson(catalogString, Catalog.class);
            serviceDefinitions.addAll(catalogObj.getServiceDefinitions());
        }
        return serviceDefinitions;
    }
}