package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common.HiveCommonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HiveAdminService implements OCDPAdminService {

    private Logger logger = LoggerFactory.getLogger(HiveAdminService.class);

    private ClusterConfig clusterConfig;

    private HiveCommonService hiveCommonService;

    @Autowired
    public HiveAdminService(ClusterConfig clusterConfig, HiveCommonService hiveCommonService){
        this.clusterConfig = clusterConfig;
        this.hiveCommonService = hiveCommonService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception{
        return hiveCommonService.createDatabase(serviceInstanceId);
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName){
        logger.info("Assign select/update/create/drop/alter/index/lock/all permission to hive database.");
        return this.hiveCommonService.assignPermissionToDatabase(policyName, resourceName, accountName, groupName);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        return this.hiveCommonService.appendUserToDatabasePermission(policyId, groupName, accountName);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.hiveCommonService.deleteDatabase(serviceInstanceResuorceName);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive table.");
        return this.hiveCommonService.unassignPermissionFromDatabase(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        return this.hiveCommonService.removeUserFromDatabasePermission(policyId, groupName, accountName);
    }

    @Override
    public String getDashboardUrl(){
        //TODO Hive 1.2.0 not support web UI, this part should be enhance in hive 2.1.
        return "";
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>(){
            {
                put("uri", hiveCommonService.getHiveJDBCUrl());
                put("username", accountName);
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getHiveHost());
                put("port", clusterConfig.getHivePort());
                put("name", serviceInstanceResource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

}
