package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.net.URI;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;

import com.google.gson.internal.LinkedTreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HDFSAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    private static final FsPermission FS_PERMISSION = new FsPermission(FsAction.ALL, FsAction.ALL,
            FsAction.NONE);

    private static final FsPermission FS_USER_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE,
            FsAction.NONE);

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private DistributedFileSystem dfs;

    private Configuration conf;

    private String hdfsRPCUrl;

    @Autowired
    public HDFSAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.dfs = new DistributedFileSystem();

        this.conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", clusterConfig.getHdfsUserKeytab());

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.hdfsRPCUrl = "hdfs://" + this.clusterConfig.getHdfsNameNode() + ":" + this.clusterConfig.getHdfsRpcPort();
    }

    private void authentication() throws Exception{
        UserGroupInformation.setConfiguration(this.conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception{
        String pathName;
        try{
            this.authentication();
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            if(bindingId == null){
                pathName = "/servicebroker/" + serviceInstanceId;
                this.dfs.mkdirs(new Path(pathName), FS_PERMISSION);
                // Only hdfs folder for service instance need set name/storage space quota
                Map<String, Long> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
                this.dfs.setQuota(new Path(pathName), quota.get("nameSpaceQuota"), quota.get("storageSpaceQuota"));
            }else {
                pathName = "/servicebroker/" + serviceInstanceId + "/" + bindingId;
                this.dfs.mkdirs(new Path(pathName), FS_USER_PERMISSION);
            }
            logger.info("Create hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
        return pathName;
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName){
        logger.info("Assign read/write/execute permission to hdfs folder.");
        ArrayList<String> pathList = new ArrayList<String>(){{add(resourceName);}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("execute");}};
        ArrayList<String> conditions = new ArrayList<String>();
        return this.rc.createHDFSPolicy(policyName,"This is HDFS Policy",clusterConfig.getClusterName()+"_hadoop",
                pathList, groupList,userList,types,conditions);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, true);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            this.authentication();
            this.dfs.initialize(URI.create(this.hdfsRPCUrl), this.conf);
            this.dfs.delete(new Path(serviceInstanceResuorceName), true);
            logger.info("Delete hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder delete fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.dfs.close();
        }
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, false);
    }

    @Override
    public String getDashboardUrl(){
        // Todo: should support multi-tent in future, each account can only see HDFS folders which belong to themself.
        return "http://" + this.clusterConfig.getHdfsNameNode() + ":50070";
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>(){
            {
                put("uri", hdfsRPCUrl + serviceInstanceResource);
                put("username", accountName);
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getHdfsNameNode());
                put("port", clusterConfig.getHdfsRpcPort());
                put("name", serviceInstanceResource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

    private boolean updateUserForResourcePermission(String policyId, String groupName, String accountName, boolean isAppend){
        String currentPolicy = this.rc.getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.updatePolicy(
                groupName, accountName, new ArrayList<String>(){{add("read");add("write");add("execute");}}, isAppend);
        return this.rc.updateV2Policy(policyId, gson.toJson(rp));
    }

    private Map<String, Long> getQuotaFromPlan(String serviceDefinitionId, String planId){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        List<String> bullets = (ArrayList)metadata.get("bullets");
        String[] nameSpaceQuota = (bullets.get(0)).split(":");
        String[] storageSpaceQuota = (bullets.get(1)).split(":");
        return new HashMap<String, Long>(){
            {
                put("nameSpaceQuota", new Long(nameSpaceQuota[1]));
                put("storageSpaceQuota", new Long(storageSpaceQuota[1]) * 1000000);
            }
        };
    }
}
