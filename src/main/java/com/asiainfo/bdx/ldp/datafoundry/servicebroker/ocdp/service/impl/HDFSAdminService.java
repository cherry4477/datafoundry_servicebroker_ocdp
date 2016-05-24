package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import java.util.List;
import java.io.IOException;
import java.net.URI;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.hdfsConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.rangerConfig;
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
    public hdfsConfig hdfsConfig;

    @Autowired
    public rangerConfig rangerConfig;

    @Override
    public void authentication() throws IOException{
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.hdfsConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.hdfsConfig.getUserKeytab());
        System.setProperty("java.security.krb5.conf", this.hdfsConfig.getKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(this.hdfsConfig.getHdfsSuperUser(), this.hdfsConfig.getUserKeytab());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public String provisionResources(String serviceInstanceId, String bindingId){
        logger.info("Create hdfs folder successful.");
        try{
            this.authentication();
        }catch (IOException e){
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.hdfsConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.hdfsConfig.getUserKeytab());
        System.setProperty("java.security.krb5.conf", this.hdfsConfig.getKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        String pathName;
        if(bindingId == null){
            pathName = "/servicebroker/" + serviceInstanceId;
        }else{
            pathName = "/servicebroker/" + serviceInstanceId + "/" + bindingId;
        }
        DistributedFileSystem dfs = new DistributedFileSystem();
        try{
            dfs.initialize(URI.create(this.hdfsConfig.getHdfsURL()), conf);
            if(bindingId == null){
                dfs.mkdirs(new Path(pathName), FS_PERMISSION);
            }else {
                dfs.mkdirs(new Path(pathName), FS_USER_PERMISSION);
            }
            dfs.setQuota(new Path(pathName), 4096, 4096);
        }catch (IOException e){
            e.printStackTrace();
        }
        return pathName;
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, List<String> groupList,
                                              List<String> userList, List<String> permList){
        rangerClient rc = rangerConfig.getRangerClient();
        return rc.createPolicy(policyName, resourceName, "Desc: HDFS policy.",
                "OCDP_hadoop", "hdfs", groupList, userList, permList);
    }

    @Override
    public boolean deprovisionResources(String serviceInstanceResuorceName){
        logger.info("Delete hdfs folder successful.");
        try{
            this.authentication();
        }catch (IOException e){
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hdfs.kerberos.principal", this.hdfsConfig.getHdfsSuperUser());
        conf.set("hdfs.keytab.file", this.hdfsConfig.getUserKeytab());
        System.setProperty("java.security.krb5.conf", this.hdfsConfig.getKrbFilePath());
        UserGroupInformation.setConfiguration(conf);
        DistributedFileSystem dfs = new DistributedFileSystem();
        try{
            dfs.initialize(URI.create(this.hdfsConfig.getHdfsURL()), conf);
            dfs.delete(new Path(serviceInstanceResuorceName));
        }catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/execute permission to hdfs folder.");
        rangerClient rc = rangerConfig.getRangerClient();
        rc.removePolicy(policyId);
    }

}
