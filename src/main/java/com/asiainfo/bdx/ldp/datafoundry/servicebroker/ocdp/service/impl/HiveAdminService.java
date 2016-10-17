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

    private HDFSAdminService hdfsAdminService;

    @Autowired
    public HiveAdminService(ClusterConfig clusterConfig, HiveCommonService hiveCommonService, HDFSAdminService hdfsAdminService){
        this.clusterConfig = clusterConfig;
        this.hiveCommonService = hiveCommonService;
        this.hdfsAdminService = hdfsAdminService;
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception{
        return hiveCommonService.createDatabase(serviceInstanceId);
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName){
        logger.info("Assign select/update/create/drop/alter/index/lock/all permission to hive database.");
        String hivePolicyId = this.hiveCommonService.assignPermissionToDatabase(policyName, resourceName, accountName, groupName);
        logger.info("Create corresponding hdfs policy for hive tenant");
        String hdfsPolicyId = this.hdfsAdminService.assignPermissionToResources(
                "hdfs_" + policyName, "/apps/hive/warehouse/" + resourceName + ".db", accountName, groupName);
        return hivePolicyId + ":" + hdfsPolicyId;
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        String[] policyIds = policyId.split(";");
        return this.hiveCommonService.appendUserToDatabasePermission(policyIds[0], groupName, accountName) &&
                this.hdfsAdminService.appendUserToResourcePermission(policyIds[1], groupName, accountName);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        this.hiveCommonService.deleteDatabase(serviceInstanceResuorceName);
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        String[] policyIds = policyId.split(":");
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive table.");
        return this.hiveCommonService.unassignPermissionFromDatabase(policyIds[0]) &&
                this.hdfsAdminService.unassignPermissionFromResources(policyIds[1]);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        String[] policyIds = policyId.split(":");
        return this.hiveCommonService.removeUserFromDatabasePermission(policyIds[0], groupName, accountName) &&
                this.hdfsAdminService.removeUserFromResourcePermission(policyIds[1], groupName, accountName);
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
                put("uri", "jdbc:hive2://" + clusterConfig.getHiveHost() + ":" +
                                clusterConfig.getHivePort() + "/" + serviceInstanceResource +
                                ";principal=" + accountName );
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
