package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.CatalogConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config.ClusterConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.CapacitySchedulerConfig;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.HDFSRangerPolicy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model.YarnRangerPolicy;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils.YarnCapacityCaculater;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.internal.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import org.apache.hadoop.yarn.client.api.YarnClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Aaron on 16/7/20.
 */
@Service
public class YarnAdminService implements OCDPAdminService {


    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    static final Gson gson = new GsonBuilder().create();

    @Autowired
    private ApplicationContext context;

    private ClusterConfig clusterConfig;

    private rangerClient rc;

    private Configuration conf;

    private ambariClient ambClient;

    private yarnClient yClient;

    private YarnCapacityCaculater capacityCaculater;

//    private YarnClient yc;

    @Autowired
    public YarnAdminService(ClusterConfig clusterConfig){
        this.clusterConfig = clusterConfig;

        this.rc = clusterConfig.getRangerClient();

        this.ambClient = clusterConfig.getAmbariClient();

        this.yClient = clusterConfig.getYarnClient();

        this.conf = new Configuration();
//        Config for Yarn Client----> TBD
//        conf.set("hadoop.security.authentication", "Kerberos");
//        conf.set("yarn.security.authentication","Kerberos");
//        conf.set("yarn.kerberos.principal", clusterConfig.getYarn_superUser());
//        conf.set("yarn.keytab.file", clusterConfig.getYarn_superUserKeytab());
//
//        conf.set(YarnConfiguration.RM_ADDRESS,clusterConfig.getYarn_rm_host());
//        conf.set(YarnConfiguration.RM_PRINCIPAL, "rm/hadoop-2.jcloud.local@ASIAINFO.COM");
//        conf.set(YarnConfiguration.RM_KEYTAB, "/tmp/rm.service.keytab");
//        conf.set(YarnConfiguration.DEFAULT_RM_ADDRESS,clusterConfig.getYarn_rm_host()+":8050");
//        conf.setInt(String.valueOf(YarnConfiguration.DEFAULT_RM_PORT),8050);

        System.setProperty("java.security.krb5.conf", clusterConfig.getKrb5FilePath());

    }


    @Override
    public void authentication() throws Exception {
        UserGroupInformation.setConfiguration(this.conf);
        try{
            UserGroupInformation.loginUserFromKeytab(
                    this.clusterConfig.getYarn_superUser(), this.clusterConfig.getYarn_superUserKeytab());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //TODO Need Sync
    @Override
    public String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId, String bindingId) throws Exception {
        String csConfig = null;
        String clusterTotalMemory = null;
//        String clusterAvailableMemory = null;
        String provisionedQueue = null;
        String queuePath = null;

        try {
            csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarn_rm_host());
            CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
            yClient.getClusterMetrics();
            clusterTotalMemory = yClient.getTotalMemory();
//            clusterAvailableMemory = yClient.getAvailableMemory();
            Map<String, Long> quota = this.getQuotaFromPlan(serviceDefinitionId, planId);
            Long queueQuota = quota.get("yarnQueueQuota");
            YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory,csActualConfig);
            provisionedQueue = capacityCaculater.applyQueue(queueQuota);
            if(provisionedQueue == null)
                throw new OCDPServiceException("Not Enough Capacity to apply!");
            queuePath = "root."+provisionedQueue;
            this.capacityCaculater = capacityCaculater;


//            Run for Yarn Client----> TBD
//            this.authentication();
//
//            ResourceManager rm = new ResourceManager();
//            rm.init(conf);
//            ResourceScheduler rs = rm.getResourceScheduler();
//            QueueMetrics metrics = rs.getRootQueueMetrics();
//
//            yc = YarnClient.createYarnClient();
//            yc.init(conf);
//            logger.info("Running Client");
//            YarnClusterMetrics clusterMetrics = yc.getYarnClusterMetrics();
//            logger.info("Got Cluster metric info from ASM"
//                    + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

            //from ActualConfig ---> NewConfig with planId


        }catch (Exception e){
            e.printStackTrace();
        }
        return queuePath;
    }

    @Override
    public String assignPermissionToResources(String policyName, final String resourceName, String accountName, String groupName) {

        logger.info("Assign submit-app/admin-queue permission to yarn queue.");

        ArrayList<String> queueList = new ArrayList<String>(){{add(resourceName);}};
        ArrayList<String> groupList = new ArrayList<String>(){{add(groupName);}};
        ArrayList<String> userList = new ArrayList<String>(){{add(accountName);}};
        ArrayList<String> types = new ArrayList<String>(){{add("submit-app");add("admin-queue");}};
        ArrayList<String> conditions = new ArrayList<String>();
        String policyId = this.rc.createYarnPolicy(policyName,"This is Yarn Policy",clusterConfig.getClusterName()+"_yarn",
                queueList,groupList,userList,types,conditions);
        if(policyId != null){
            this.capacityCaculater.addQueueMapping(accountName,resourceName);
            ambClient.updateCapacitySchedulerConfig(this.capacityCaculater.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarn_rm_host());

            logger.info("Complete refresh yarn queues.");
        }
        return policyId;

    }

    @Override
    public boolean appendUserToResourcePermission(String policyId, String groupName, String accountName) {
        return updateUserForResourcePermission(policyId,groupName,accountName,true);
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName) throws Exception {

        String csConfig = null;
        String clusterTotalMemory = null;
//        String provisionedQueue = null;

        try{
            csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarn_rm_host());
            CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
            yClient.getClusterMetrics();
            clusterTotalMemory = yClient.getTotalMemory();
            YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory,csActualConfig);

            capacityCaculater.revokeQueue(serviceInstanceResuorceName);
//            this.capacityCaculater = capacityCaculater;
            capacityCaculater.removeQueueMapping(serviceInstanceResuorceName);
            ambClient.updateCapacitySchedulerConfig(capacityCaculater.getProperties(),clusterConfig.getClusterName());
            ambClient.refreshYarnQueue(clusterConfig.getYarn_rm_host());
            logger.info("Complete refresh yarn queues.");

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public boolean unassignPermissionFromResources(String policyId) {
//        String currentPolicy = this.rc.getV2Policy(policyId);
//        if(currentPolicy != null){
//            YarnRangerPolicy rp = gson.fromJson(currentPolicy,YarnRangerPolicy.class);
//            List<String> users = rp.getUserList();
//
//        }
        logger.info("Unassign permission to yarn queue.");
        return this.rc.removeV2Policy(policyId);
    }

    @Override
    public boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName) {
        return updateUserForResourcePermission(policyId,groupName,accountName,false);
    }

    private boolean updateUserForResourcePermission(String policyId, String groupName, String accountName, boolean isAppend){

        String currentPolicy = this.rc.getV2Policy(policyId);
        if (currentPolicy == null)
        {
            return false;
        }
        YarnRangerPolicy rp = gson.fromJson(currentPolicy, YarnRangerPolicy.class);
        rp.updatePolicy(
                groupName, accountName, new ArrayList<String>(){{add("submit-app");add("admin-queue");}}, isAppend);
        String queueName = rp.getResourceValues().get(0);
        boolean updateStatus = this.rc.updateV2Policy(policyId, gson.toJson(rp));
        if(updateStatus) {
            try {
                String csConfig = ambClient.getCapacitySchedulerConfig(clusterConfig.getYarn_rm_host());
                CapacitySchedulerConfig csActualConfig = gson.fromJson(csConfig, CapacitySchedulerConfig.class);
                yClient.getClusterMetrics();
                String clusterTotalMemory = yClient.getTotalMemory();
                YarnCapacityCaculater capacityCaculater = new YarnCapacityCaculater(clusterTotalMemory, csActualConfig);
                if (isAppend) {
                    capacityCaculater.addQueueMapping(accountName, queueName);
                } else {
                    capacityCaculater.removeQueueMapping(accountName, queueName);
                }
                ambClient.updateCapacitySchedulerConfig(capacityCaculater.getProperties(),clusterConfig.getClusterName());
                ambClient.refreshYarnQueue(clusterConfig.getYarn_rm_host());
                logger.info("Complete refresh yarn queues.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return updateStatus;
    }

    @Override
    public String getDashboardUrl() {
        return null;
    }

    @Override
    public Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                       String serviceInstanceResource, String rangerPolicyId){
        return new HashMap<>();
    }

    private Map<String, Long> getQuotaFromPlan(String serviceDefinitionId, String planId){
        CatalogConfig catalogConfig = (CatalogConfig) this.context.getBean("catalogConfig");
        Plan plan = catalogConfig.getServicePlan(serviceDefinitionId, planId);
        Map<String, Object> metadata = plan.getMetadata();
        String yarnQueueQuota = (String)((LinkedTreeMap)((ArrayList)metadata.get("bullets")).get(0)).get("Yarn Queue Quota (GB)");
        return new HashMap<String, Long>(){
            {
                put("yarnQueueQuota", new Long(yarnQueueQuota));
            }
        };
    }
}
