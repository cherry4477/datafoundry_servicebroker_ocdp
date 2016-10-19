package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.HiveCommonService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.YarnCommonService;
import com.google.gson.internal.LinkedTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class SparkAdminService implements OCDPAdminService {
    private Logger logger = LoggerFactory.getLogger(SparkAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    private HiveCommonService hiveCommonService;

    @Autowired
    public SparkAdminService(ClusterConfig clusterConfig, YarnCommonService yarnCommonService, HiveCommonService hiveCommonService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
        this.hiveCommonService = hiveCommonService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception {
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
        String queueName = yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
        String dbName = hiveCommonService.createDatabase(serviceInstanceId);
        // return yarn queue name and hive database, because spark need both resources
        return queueName + ":" + dbName;
    }

    @Override
    public String assignPermissionToResources(String policyName, final List<String> resources, String accountName, String groupName) {
        logger.info("Assign permissions for yarn queue and hive database.");
        String[] resourcesList = resources.get(0).split(":");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resourcesList[0], accountName, groupName);
        String hiveId = this.hiveCommonService.assignPermissionToDatabase(policyName, resourcesList[1], accountName, groupName);
        // return yarn policy id and hive policy id, because spark need both resources
        return yarnPolicyId + ":" + hiveId;
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        String[] policyIds = policyId.split(":");
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(policyIds[0], groupName, accountName);
        boolean userAppendToHivePolicy = this.hiveCommonService.appendUserToDatabasePermission(policyIds[1], groupName, accountName);
        return userAppendToYarnPolicy && userAppendToHivePolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.yarnCommonService.deleteQueue(resources[0]);
        this.hiveCommonService.deleteDatabase(resources[1]);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
        logger.info("Unassign permission to yarn queue and hive database.");
        String[] policyIds = policyId.split(":");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[0]);
        boolean hivePolicyDeleted = this.hiveCommonService.unassignPermissionFromDatabase(policyIds[1]);
        return yarnPolicyDeleted && hivePolicyDeleted;
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(policyIds[0], groupName, accountName);
        boolean userRemovedFromHivePolicy = this.hiveCommonService.removeUserFromDatabasePermission(policyIds[1], groupName, accountName);
        return userRemovedFromYarnPolicy && userRemovedFromHivePolicy;
    }

    @Override
    public String getDashboardUrl() {
        return this.clusterConfig.getSparkHistoryURL();
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>(){
            {
                put("uri", clusterConfig.getYarnRMUrl());
                put("username", accountName);
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getYarnRMHost());
                put("port", clusterConfig.getYarnRMPort());
                put("name", serviceInstanceResource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        List<String> bullets = (ArrayList)metadata.get("bullets");
        String[] yarnQueueQuota = (bullets.get(0)).split(":");
        return new HashMap<String, String>(){
            {
                put("yarnQueueQuota", yarnQueueQuota[1]);
            }
        };
    }
}
