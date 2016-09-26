package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.PlanMetadata;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.RangerV2Policy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HBaseAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HBaseAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private Configuration conf;

    private Connection connection;

    @Autowired
    public HBaseAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        System.setProperty("java.security.krb5.conf", this.clusterConfig.getKrb5FilePath());
        this.conf = HBaseConfiguration.create();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.master.kerberos.principal", this.clusterConfig.getHbaseMasterPrincipal());
        conf.set("hbase.master.keytab.file", this.clusterConfig.getHbaseMasterUserKeytab());
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.clusterConfig.getHbaseZookeeperQuorum());
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.clusterConfig.getHbaseZookeeperClientPort());
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.clusterConfig.getHbaseZookeeperZnodeParent());
    }

    private void authentication(){
        UserGroupInformation.setConfiguration(this.conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getHbaseMasterPrincipal(), this.clusterConfig.getHbaseMasterUserKeytab());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception{
        String nsName = serviceInstanceId.replaceAll("-", "");
        Map<String, String> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
        try{
            this.authentication();
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = this.connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();
            namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxtables", quota.get("maximumTableQuota"));
            namespaceDescriptor.setConfiguration("hbase.namespace.quota.maxregion", quota.get("maximunRegionQuota"));
            admin.createNamespace(namespaceDescriptor);
            admin.close();
        }catch(IOException e){
            logger.error("HBase namespace create fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        }finally {
            this.connection.close();
        }
        return nsName;
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName){
        logger.info("Assign read/write/create/admin permission to hbase namespace.");
        ArrayList<String> nsList = new ArrayList<String>(){{add(resourceName + ":*");}};
        ArrayList<String> cfList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> cList = new ArrayList<String>(){{add("*");}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("read");add("write");add("create");add("admin");}};
        ArrayList<String> conditions = new ArrayList<String>();
        return this.rc.createHBasePolicy(policyName,"This is HBase Policy", clusterConfig.getClusterName()+"_hbase",
                nsList, cfList, cList, groupList,userList,types,conditions);
    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, true);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception{
        try{
            this.authentication();
            this.connection = ConnectionFactory.createConnection(conf);
            Admin admin = this.connection.getAdmin();
            admin.deleteNamespace(serviceInstanceResuorceName);
            admin.close();
            logger.info("Delete HBase namespace successful.");
        }catch (IOException e){
            logger.error("HBase namespace delete fail due to: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw e;
        } finally {
            this.connection.close();
        }
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/create/admin permission to hbase namespace.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName){
        return this.updateUserForResourcePermission(policyId, groupName, accountName, false);
    }

    @Override
    public String getDashboardUrl(){
        // Todo: should support multi-tent in future, each account can only see HBase namespace which belong to themself.
        return this.clusterConfig.getHbaseMasterUrl();
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<String, Object>(){
            {
                put("uri", clusterConfig.getHbaseZookeeperQuorum() + ":" +
                        clusterConfig.getHbaseZookeeperClientPort() + ":" + clusterConfig.getHbaseZookeeperZnodeParent());
                put("username", accountName);
                put("password", accountPwd);
                put("keytab", accountKeytab);
                put("host", clusterConfig.getHbaseZookeeperQuorum());
                put("port", clusterConfig.getHbaseZookeeperClientPort());
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
                groupName, accountName, new ArrayList<String>(){{add("read"); add("write"); add("create"); add("admin");}}, isAppend);
        return this.rc.updateV2Policy(policyId, gson.toJson(rp));
    }

    private Map<String, String> getQuotaFromPlan(String serviceDefinitionId, String planId){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        List<String> bullets = (ArrayList)metadata.get("bullets");
        String[] maximumTableQuota = (bullets.get(0)).split(":");
        String[] maximunRegionQuota = (bullets.get(1)).split(":");
        return new HashMap<String, String>(){
            {
                put("maximumTableQuota", maximumTableQuota[1]);
                put("maximunRegionQuota", maximunRegionQuota[1]);
            }
        };
    }
}
