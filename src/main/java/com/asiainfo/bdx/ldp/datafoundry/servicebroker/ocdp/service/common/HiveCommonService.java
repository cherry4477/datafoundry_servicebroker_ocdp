package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.common;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created by baikai on 8/8/16.
 */
@Service
public class HiveCommonService {

    private Logger logger = LoggerFactory.getLogger(HiveCommonService.class);

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
    public HiveCommonService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

        this.hiveJDBCUrl = "jdbc:hive2://" + this.clusterConfig.getHiveHost() + ":" + this.clusterConfig.getHivePort() +
                "/default;principal=" + this.clusterConfig.getHiveSuperUser();
    }

    private void authentication(){
        UserGroupInformation.setConfiguration(this.conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getHiveSuperUser(), this.clusterConfig.getHiveSuperUserKeytab());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public String createDatabase(String serviceInstanceId) throws Exception{
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

    public String assignPermissionToDatabase(String policyName, final String dbName, String accountName, String groupName){
        ArrayList<String> dbList = new ArrayList<String>(){{add(dbName);}};
        ArrayList<String> cfList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> cList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("select"); add("update");
            add("create"); add("drop"); add("alter"); add("index"); add("lock"); add("all");}};
        ArrayList<String> conditions = new ArrayList<String>();
        return this.rc.createHivePolicy(policyName,"This is Hive Policy", clusterConfig.getClusterName()+"_hive",
                dbList, cfList, cList, groupList,userList,types,conditions);
    }

    public boolean appendUserToDatabasePermission(String policyId, String groupName, String accountName) {
        return this.updateUserForResourcePermission(policyId, groupName, accountName, true);
    }

    public void deleteDatabase(String dbName) throws Exception{
        try{
            this.authentication();
            Class.forName(this.driverName);
            this.conn = DriverManager.getConnection(this.hiveJDBCUrl);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database " + dbName);
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

    public boolean unassignPermissionFromDatabase(String policyId){
        return this.rc.removeV2Policy(policyId);
    }

    public boolean removeUserFromDatabasePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, false);
    }

    public String getHiveJDBCUrl(){
        return this.hiveJDBCUrl;
    }

    private boolean updateUserForResourcePermission(String policyId, String groupName, String accountName, boolean isAppend){
        String currentPolicy = this.rc.getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        RangerV2Policy rp = gson.fromJson(currentPolicy, RangerV2Policy.class);
        rp.updatePolicy(
                groupName, accountName, new ArrayList<String>(){{add("select"); add("update");
                    add("create"); add("drop"); add("alter"); add("index"); add("lock"); add("all");}}, isAppend);
        return this.rc.updateV2Policy(policyId, gson.toJson(rp));
    }

}
