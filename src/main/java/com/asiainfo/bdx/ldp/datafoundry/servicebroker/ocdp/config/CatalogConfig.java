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

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.OCDPAdminServiceMapper;

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

    private Catalog getServiceCatalog(){
        Catalog catalog = null;
        ClusterConfig clusterConfig = (ClusterConfig)this.context.getBean("clusterConfig");
        etcdClient etcdClient = clusterConfig.getEtcdClient();
        List<ServiceDefinition> sds = new ArrayList<>();
        for(String id : OCDPAdminServiceMapper.getOCDPServiceIds()){
            if (etcdClient.read("/servicebroker/ocdp/catalog/" + id) == null){
                continue;
            }
            String name = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/name");
            String description = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/description");
            String bindable = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/bindable");
            //String planupdatable = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/planupdatable");
            //String tags = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/tags");
            //String metadata = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/metadata");
            String planId = OCDPAdminServiceMapper.getOCDPServicePlan(id);
            String planName = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/name");
            String planDescription = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/description");
            String planFree = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/free");
            String planMetadata = etcdClient.readToString("/servicebroker/ocdp/catalog/" + id + "/plan/" + planId + "/metadata");
            PlanMetadata planMetadataObj = gson.fromJson(planMetadata, PlanMetadata.class);
            Map<String, Object> planMetadataMap = new HashMap<String, Object>() {
                {
                    put("costs", planMetadataObj.getCosts());
                    put("bullets", planMetadataObj.getBullets());
                }
            };
            Plan plan = new Plan(planId, planName, planDescription, planMetadataMap, Boolean.getBoolean(planFree));
            List<Plan> plans = new ArrayList<Plan>(){
                {
                    add(plan);
                }
            };
            ServiceDefinition sd = new ServiceDefinition(id, name, description, Boolean.getBoolean(bindable), plans);
            sds.add(sd);
        }
        if (sds.size() != 0){
            catalog = new Catalog(sds);
        }
        return catalog;
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