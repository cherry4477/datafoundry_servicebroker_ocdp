package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.util.*;

import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.*;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

@Configuration
public class CatalogConfig {

    @Autowired
    private ApplicationContext context;

    static final Gson gson = new GsonBuilder().create();

    @Bean
    public Catalog catalog() {
        return new Catalog(
                this.getServiceDefinitions()
        );
    }

    private List<ServiceDefinition> getServiceDefinitions() {
        ArrayList<ServiceDefinition> serviceDefinitions = new ArrayList<ServiceDefinition>();
        ClusterConfig clusterConfig = (ClusterConfig)this.context.getBean("clusterConfig");
        etcdClient etcdClient = clusterConfig.getEtcdClient();
        String catalogString = etcdClient.readToString("/servicebroker/ocdp/catalog");
        if (catalogString != null) {
            Catalog catalogObj = gson.fromJson(catalogString, Catalog.class);
            serviceDefinitions.addAll(catalogObj.getServiceDefinitions());
        }
        return serviceDefinitions;
    }
}