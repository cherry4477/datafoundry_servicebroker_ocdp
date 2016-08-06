package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.YarnCommonService;
import com.google.gson.internal.LinkedTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 8/4/16.
 */
@Service
public class MapReduceAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    @Autowired
    public MapReduceAdminService(ClusterConfig clusterConfig, YarnCommonService yarnCommonService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception {
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
        return this.yarnCommonService.createQueue(quota.get("yarnQueueQuota"));
    }

    @Override
    public String assignPermissionToResources(String policyName, final String resourceName, String accountName, String groupName) {
        logger.info("Assign submit-app/admin-queue permission to yarn queue.");
        return this.yarnCommonService.assignPermissionToQueue(policyName, resourceName, accountName, groupName);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        return this.yarnCommonService.appendUserToQueuePermission(policyId, groupName, accountName);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.yarnCommonService.deleteQueue(serviceInstanceResuorceName);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
        logger.info("Unassign permission to yarn queue.");
        return this.yarnCommonService.unassignPermissionFromQueue(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        return this.yarnCommonService.removeUserFromQueuePermission(policyId, groupName, accountName);
    }

    @Override
    public String getDashboardUrl() {
        return this.clusterConfig.getMRHistoryURL();
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
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        String yarnQueueQuota = (String)((LinkedTreeMap)((ArrayList)metadata.get("bullets")).get(0)).get("Yarn Queue Quota (GB)");
        return new HashMap<String, String>(){
            {
                put("yarnQueueQuota", yarnQueueQuota);
            }
        };
    }

}
