package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.util.*;

import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.Plan;
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

    private Catalog getServiceCatalog(){
        Catalog catalog = null;
        ClusterConfig clusterConfig = (ClusterConfig)this.context.getBean("clusterConfig");
        etcdClient etcdClient = clusterConfig.getEtcdClient();
        String catalogString = etcdClient.readToString("/servicebroker/ocdp/catalog");
        if (catalogString != null){
            catalog = gson.fromJson(catalogString, Catalog.class);
        }
        return catalog;
    }

    public ServiceDefinition getServiceDefinition(String serviceDefinitionId){
        ServiceDefinition serviceDefinition = null;
        Catalog catalog = this.getServiceCatalog();
        if(catalog != null){
            for(ServiceDefinition sd : catalog.getServiceDefinitions()){
                if( (sd.getId()).equals(serviceDefinitionId) ){
                    serviceDefinition = sd;
                    break;
                }
            }
        }
        return serviceDefinition;
    }

    public Plan getServicePlan(String serviceDefinitionId, String planId){
        Plan plan = null;
        ServiceDefinition sd = getServiceDefinition(serviceDefinitionId);
        if (sd != null){
            for(Plan p : sd.getPlans()){
                if((p.getId()).equals(planId)){
                    plan = p;
                    break;
                }
            }
        }
        return plan;
    }

    private List<ServiceDefinition> getServiceDefinitions() {
        ArrayList<ServiceDefinition> serviceDefinitions = new ArrayList<ServiceDefinition>();
        Catalog catalog = getServiceCatalog();
        if(catalog != null){
            serviceDefinitions.addAll(catalog.getServiceDefinitions());
        }
        return serviceDefinitions;
    }
}