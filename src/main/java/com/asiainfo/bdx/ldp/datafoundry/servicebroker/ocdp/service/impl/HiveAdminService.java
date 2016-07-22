package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.HiveRangerPolicy;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HiveAdminService implements OCDPAdminService {

    private Logger logger = LoggerFactory.getLogger(HiveAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private Configuration conf;

    private Connection conn;

    private String hiveJDBCUrl;

    @Autowired
    public HiveAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.hiveJDBCUrl = "jdbc:hive2://" + this.clusterConfig.getHiveHost() + ":" + this.clusterConfig.getHivePort() +
                "/default;principal=" + this.clusterConfig.getHiveSuperUser();
    }

    @Override
    public void authentication(){
        UserGroupInformation.setConfiguration(this.conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getHiveSuperUser(), this.clusterConfig.getHiveSuperUserKeytab());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception{
        String databaseName = serviceInstanceId.replaceAll("-", "");
        try{
            this.authentication();
            Class.forName(this.driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl);
            Statement stmt = conn.createStatement();
            stmt.execute("create database " + databaseName);
        }catch (ClassNotFoundException e){
            logger.error("Hive JDBC driver not found in classpath.");
            e.printStackTrace();
            throw e;
        }
        catch(SQLException e){
            logger.error("Hive database create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
        }
        return databaseName;
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName){
        logger.info("Assign select/update/create/drop/alter/index/lock/all permission to hive database.");
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> permList = new ArrayList<String>(){{add("select"); add("update");
            add("create"); add("drop"); add("alter"); add("index"); add("lock"); add("all");}};
        return this.rc.createHivePolicy(policyName, resourceName, "*", "*", "Desc: Hive policy.",
                this.clusterConfig.getClusterName() + "_hive", "hive", groupList, userList, permList);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, true);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName)throws Exception{
        try{
            this.authentication();
            Class.forName(this.driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database " + serviceInstanceResuorceName);
        }catch (ClassNotFoundException e){
            logger.error("Hive JDBC driver not found in classpath.");
            e.printStackTrace();
            throw e;
        }catch (SQLException e){
            logger.error("Hive database drop fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            conn.close();
        }
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign select/update/create/drop/alter/index/lock/all permission to hive table.");
        return this.rc.removePolicy(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, false);
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
                put("uri", hiveJDBCUrl);
                put("username", accountName);
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getHiveHost());
                put("port", clusterConfig.getHivePort());
                put("resource", serviceInstanceResource);
                put("rangerPolicyId", rangerPolicyId);
            }
        };
    }

    private boolean updateUserForResourcePermission(String policyId, String groupName, String accountName, boolean isAppend){
        String currentPolicy = this.rc.getPolicy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        HiveRangerPolicy rp = gson.fromJson(currentPolicy, HiveRangerPolicy.class);
        rp.updatePolicyPerm(
                groupName, accountName, new ArrayList<String>(){{add("select"); add("update");
                    add("create"); add("drop"); add("alter"); add("index"); add("lock"); add("all");}}, isAppend);
        return this.rc.updatePolicy(policyId, gson.toJson(rp));
    }

}
