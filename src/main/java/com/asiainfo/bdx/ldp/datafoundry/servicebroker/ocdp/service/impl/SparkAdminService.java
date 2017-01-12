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

    private HDFSAdminService hdfsAdminService;

    @Autowired
    public SparkAdminService(ClusterConfig clusterConfig,
                             YarnCommonService yarnCommonService, HDFSAdminService hdfsAdminService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
        this.hdfsAdminService = hdfsAdminService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                                     String bindingId, String accountName) throws Exception {
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
        String queueName = yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
        String dirName = "/user/" + accountName;
        this.hdfsAdminService.createHDFSDir(dirName, new Long(quota.get("nameSpaceQuota")), new Long(quota.get("storageSpaceQuota")) * 1000000000);
        // return yarn queue name and hive database, because spark need both resources
        return queueName + ":" + dirName;
    }

    @Override
    public String assignPermissionToResources(String policyName, final List<String> resources, String accountName, String groupName) {
        String[] resourcesList = resources.get(0).split(":");
        logger.info("Assign permissions for yarn queue.");
        String yarnPolicyId = this.yarnCommonService.assignPermissionToQueue(policyName, resourcesList[0], accountName, groupName);
        logger.info("Create corresponding hdfs policy for spark tenant");
        List<String> hdfsFolders = new ArrayList<String>(){
            {
                add("/user/" + accountName);
            }
        };
        String hdfsPolicyId = this.hdfsAdminService.assignPermissionToResources("spark_" + policyName, hdfsFolders, accountName, groupName);
        // return yarn policy id and hive policy id, because spark need both resources
        return (yarnPolicyId != null && hdfsPolicyId != null) ? yarnPolicyId + ":" + hdfsPolicyId : null;
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        String[] policyIds = policyId.split(":");
        boolean userAppendToYarnPolicy = this.yarnCommonService.appendUserToQueuePermission(policyIds[0], groupName, accountName);
        boolean userAppendToHDFSPolicy = this.hdfsAdminService.appendUserToResourcePermission(policyIds[1], groupName, accountName);
        return userAppendToYarnPolicy && userAppendToHDFSPolicy;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        String[] resources = serviceInstanceResuorceName.split(":");
        this.yarnCommonService.deleteQueue(resources[0]);
        this.hdfsAdminService.deprovisionResources(resources[1]);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
        String[] policyIds = policyId.split(":");
        logger.info("Unassign submit/admin permission to yarn queue.");
        boolean yarnPolicyDeleted = this.yarnCommonService.unassignPermissionFromQueue(policyIds[0]);
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        boolean hdfsPolicyDeleted = this.hdfsAdminService.unassignPermissionFromResources(policyIds[1]);
        return yarnPolicyDeleted && hdfsPolicyDeleted;
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        String[] policyIds = policyId.split(":");
        boolean userRemovedFromYarnPolicy = this.yarnCommonService.removeUserFromQueuePermission(policyIds[0], groupName, accountName);
        boolean userRemovedFromHDFSPolicy = this.hdfsAdminService.removeUserFromResourcePermission(policyIds[1], groupName, accountName);
        return userRemovedFromYarnPolicy && userRemovedFromHDFSPolicy;
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
        String[] nameSpaceQuota = (bullets.get(2)).split(":");
        String[] storageSpaceQuota = (bullets.get(3)).split(":");
        return new HashMap<String, String>(){
            {
                put("yarnQueueQuota", yarnQueueQuota[1]);
                put("nameSpaceQuota", nameSpaceQuota[1]);
                put("storageSpaceQuota", storageSpaceQuota[1]);
            }
        };
    }
}
