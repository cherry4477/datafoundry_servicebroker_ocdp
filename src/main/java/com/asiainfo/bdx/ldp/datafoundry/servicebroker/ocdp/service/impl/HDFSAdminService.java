package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.net.URI;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;

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

    private static final FsPermission FS_PERMISSION = new FsPermission(FsAction.ALL, FsAction.ALL,
            FsAction.NONE);

    private static final FsPermission FS_USER_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE,
            FsAction.NONE);

    @Autowired
    private ClusterConfig clusterConfig;

    @Override
    public void authentication() throws Exception{
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.clusterConfig.getHdfsUserKeytab());
        System.setProperty("java.security.krb5.conf", this.clusterConfig.getHdfsKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getHdfsSuperUser(), this.clusterConfig.getHdfsUserKeytab());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String provisionResources(String serviceInstanceId, String bindingId) throws Exception{
        try{
            this.authentication();
        }catch (IOException e){
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.clusterConfig.getHdfsUserKeytab());
        System.setProperty("java.security.krb5.conf", this.clusterConfig.getHdfsKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        String pathName;
        DistributedFileSystem dfs = new DistributedFileSystem();
        try{
            dfs.initialize(URI.create(this.clusterConfig.getHdfsUrl()), conf);
            if(bindingId == null){
                pathName = "/servicebroker/" + serviceInstanceId;
                dfs.mkdirs(new Path(pathName), FS_PERMISSION);
                // Only hdfs folder for service instance need set name/storage space quota
                Map<String, Long> quota = this.getQuotaFromPlan();
                dfs.setQuota(new Path(pathName), quota.get("nameSpaceQuota"), quota.get("storageSpaceQuota"));
            }else {
                pathName = "/servicebroker/" + serviceInstanceId + "/" + bindingId;
                dfs.mkdirs(new Path(pathName), FS_USER_PERMISSION);
            }
            logger.info("Create hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }
        return pathName;
    }

    @Override
    public boolean assignPermissionToResources(String policyName, String resourceName, List<String> groupList,
                                              List<String> userList, List<String> permList){
        rangerClient rc = clusterConfig.getRangerClient();
        return rc.createPolicy(policyName, resourceName, "Desc: HDFS policy.",
                "OCDP_hadoop", "hdfs", groupList, userList, permList);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            this.authentication();
        }catch (IOException e){
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.clusterConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.clusterConfig.getHdfsUserKeytab());
        System.setProperty("java.security.krb5.conf", this.clusterConfig.getHdfsKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        DistributedFileSystem dfs = new DistributedFileSystem();
        try{
            dfs.initialize(URI.create(this.clusterConfig.getHdfsUrl()), conf);
            dfs.delete(new Path(serviceInstanceResuorceName));
            logger.info("Delete hdfs folder successful.");
        }catch (Exception e){
            logger.error("HDFS folder delete fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        rangerClient rc = clusterConfig.getRangerClient();
        return rc.removePolicy(policyId);
    }

    private Map<String, Long> getQuotaFromPlan(){
        return new HashMap<String, Long>(){
            {
                put("nameSpaceQuota", new Long(1000));
                put("storageSpaceQuota", new Long(10000000));
            }
        };
    }

}
