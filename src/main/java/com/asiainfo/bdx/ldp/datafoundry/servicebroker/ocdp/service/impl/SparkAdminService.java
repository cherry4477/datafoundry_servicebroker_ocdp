package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.YarnCommonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 8/4/16.
 */
public class SparkAdminService implements OCDPAdminService {
    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private YarnCommonService yarnCommonService;

    @Autowired
    public SparkAdminService(ClusterConfig clusterConfig, YarnCommonService yarnCommonService){
        this.clusterConfig = clusterConfig;
        this.yarnCommonService = yarnCommonService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception {
        return "";
    }

    @Override
    public String assignPermissionToResources(String policyName, final String resourceName, String accountName, String groupName) {
        logger.info("Assign submit-app/admin-queue permission to yarn queue.");
        return "";
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        return true;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{

    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
        logger.info("Unassign permission to yarn queue.");
        return true;
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        return true;
    }

    @Override
    public String getDashboardUrl() {
        return "";
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>();
    }
}
