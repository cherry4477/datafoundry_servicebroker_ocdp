package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.ambariClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.yarnClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.context.EnvironmentAware;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;


/**
 * Created by baikai on 5/28/16.
 */
@Configuration
public class ClusterConfig implements EnvironmentAware{

    // Etcd connectivity properties
    private String etcd_host;

    private String etcd_port;

    private String etcd_user;

    private String etcd_pwd;

    // LDAP connectivity properties
    private String ldap_url;

    private String ldap_userDN;

    private String ldap_password;

    private String ldap_base;

    private String ldap_group;

    private String ldap_group_id;

    // Kerberos connectivity properties
    private String krb_kdcHost;

    private String krb_userPrincipal;

    private String krb_keytabLocation;

    private String krb_adminPwd;

    private String krb_realm;

    private String krb_krb5FilePath;

    // Hadoop cluster name
    private String cluster_name;

    // Hadoop Ranger connectivity properties
    private String ranger_url;

    private String ranger_user;

    private String ranger_pwd;

    // Hadoop HDFS connectivity properties
    private String hdfs_nameNode;

    private String hdfs_rpcPort;

    private String hdfs_port;

    private String hdfs_superUser;

    private String hdfs_userKeytab;

    // Hadoop HBase connectivity properties
    private String hbase_masterUrl;

    private String hbase_masterPrincipal;

    private String hbase_masterUserKeytab;

    private String hbase_zookeeper_quorum;

    private String hbase_zookeeper_clientPort;

    private String hbase_zookeeper_znodeParent;

    private String hbase_master;

    private String hbase_restPort;

    // Hadoop Hive connectivity properties
    private String hive_host;

    private String hive_port;

    private String hive_superUser;

    private String hive_superUserKeytab;

    //Hadoop Ambari connectivity properties
    private String ambari_host;

    private String ambari_adminUser;

    private String ambari_adminPwd;

    //Hadoop Yarn Resource Manager properties
    private String yarn_rm_host;

    private String yarn_rm_port;

    private String yarn_rm_url;

    private String yarn_superUser;

    private String yarn_superUserKeytab;

    //Hadoop MapReduce History server
    private String mr_history_url;

    //Hadoop Spark History server
    private String spark_history_url;

    @Override
    public void setEnvironment(Environment env){
        this.etcd_host = env.getProperty("ETCD_HOST");
        this.etcd_port = env.getProperty("ETCD_PORT");
        this.etcd_user = env.getProperty("ETCD_USER");
        this.etcd_pwd = env.getProperty("ETCD_PWD");
        this.ldap_url = env.getProperty("LDAP_URL");
        this.ldap_userDN = env.getProperty("LDAP_USER_DN");
        this.ldap_password = env.getProperty("LDAP_PASSWORD");
        this.ldap_base = env.getProperty("LDAP_BASE");
        this.ldap_group = env.getProperty("LDAP_GROUP");
        this.ldap_group_id = env.getProperty("LDAP_GROUP_ID");
        this.krb_kdcHost = env.getProperty("KRB_KDC_HOST");
        this.krb_userPrincipal = env.getProperty("KRB_USER_PRINCIPAL");
        this.krb_keytabLocation = env.getProperty("KRB_KEYTAB_LOCATION");
        this.krb_adminPwd = env.getProperty("KRB_ADMIN_PASSWORD");
        this.krb_realm = env.getProperty("KRB_REALM");
        this.krb_krb5FilePath = env.getProperty("KRB_KRB5FILEPATH");
        this.cluster_name = env.getProperty("CLUSTER_NAME");
        this.ranger_url = env.getProperty("RANGER_URL");
        this.ranger_user = env.getProperty("RANGER_ADMIN_USER");
        this.ranger_pwd = env.getProperty("RANGER_ADMIN_PASSWORD");
        this.hdfs_nameNode = env.getProperty("HDFS_NAME_NODE");
        this.hdfs_rpcPort = env.getProperty("HDFS_RPC_PORT");
        this.hdfs_port = env.getProperty("HDFS_PORT");
        this.hdfs_superUser = env.getProperty("HDFS_SUPER_USER");
        this.hdfs_userKeytab = env.getProperty("HDFS_USER_KEYTAB");
        this.hbase_masterUrl = env.getProperty("HBASE_MASTER_URL");
        this.hbase_masterPrincipal = env.getProperty("HBASE_MASTER_PRINCIPAL");
        this.hbase_masterUserKeytab = env.getProperty("HBASE_MASTER_USER_KEYTAB");
        this.hbase_zookeeper_quorum = env.getProperty("HBASE_ZOOKEEPER_QUORUM");
        this.hbase_zookeeper_clientPort = env.getProperty("HBASE_ZOOKEEPER_CLIENT_PORT");
        this.hbase_zookeeper_znodeParent = env.getProperty("HBASE_ZOOKEEPER_ZNODE_PARENT");
        this.hbase_master = env.getProperty("HBASE_MASTER");
        this.hbase_restPort = env.getProperty("HBASE_REST_PORT");
        this.hive_host = env.getProperty("HIVE_HOST");
        this.hive_port = env.getProperty("HIVE_PORT");
        this.hive_superUser = env.getProperty("HIVE_SUPER_USER");
        this.hive_superUserKeytab = env.getProperty("HIVE_SUPER_USER_KEYTAB");
        this.ambari_host = env.getProperty("AMBARI_HOST");
        this.ambari_adminUser = env.getProperty("AMBARI_ADMIN_USER");
        this.ambari_adminPwd = env.getProperty("AMBARI_ADMIN_PWD");
        this.yarn_rm_host = env.getProperty("YARN_RESOURCEMANAGER_HOST");
        this.yarn_rm_port = env.getProperty("YARN_RESOURCEMANAGER_PORT");
        this.yarn_rm_url = env.getProperty("YARN_RESOURCEMANAGER_URL");
        this.yarn_superUser = env.getProperty("YARN_SUPER_USER");
        this.yarn_superUserKeytab = env.getProperty("YARN_SUPER_USER_KEYTAB");
        this.mr_history_url = env.getProperty("MR_HISTORY_URL");
        this.spark_history_url = env.getProperty("SPARK_HISTORY_URL");
    }

    public String getEtcdHost() { return this.etcd_host; }
    public String getEtcdPort() { return this.etcd_port; }
    public String getEtcdUser() { return this.etcd_user; }
    public String getEtcd_pwd() { return this.etcd_pwd; }

    public String getLdapUrl() { return this.ldap_url; }
    public String getLdapUserDN() { return this.ldap_userDN; }
    public String getLdapPassword() { return this.ldap_password; }
    public String getLdapBase() { return this.ldap_base; }
    public String getLdapGroup() { return this.ldap_group; }
    public String getLdapGroupId() { return this.ldap_group_id; }

    public String getKrbKdcHost() { return this.krb_kdcHost; }
    public String getKrbUserPrincipal() { return this.krb_userPrincipal; }
    public String getKrbKeytabLocation() { return this.krb_keytabLocation; }
    public String getKrbAdminPwd() { return this.krb_adminPwd; }
    public String getKrbRealm() { return this.krb_realm; }
    public String getKrb5FilePath() { return this.krb_krb5FilePath; }

    public String getClusterName() { return this.cluster_name; }
    public String getRangerUrl() { return this.ranger_url; }
    public String getRangerUser() { return this.ranger_user; }
    public String getRangerPwd() { return this.ranger_pwd; }

    public String getHdfsNameNode() { return this.hdfs_nameNode;}
    public String getHdfsRpcPort() { return this.hdfs_rpcPort; }
    public String getHdfsPort() { return this.hdfs_port; }
    public String getHdfsSuperUser() { return this.hdfs_superUser; }
    public String getHdfsUserKeytab() { return this.hdfs_userKeytab; }

    public String getHbaseMasterUrl() { return this.hbase_masterUrl; }
    public String getHbaseMasterPrincipal() { return this.hbase_masterPrincipal; }
    public String getHbaseMasterUserKeytab() { return this.hbase_masterUserKeytab; }
    public String getHbaseZookeeperQuorum() { return this.hbase_zookeeper_quorum; }
    public String getHbaseZookeeperClientPort() { return this.hbase_zookeeper_clientPort; }
    public String getHbaseZookeeperZnodeParent() { return this.hbase_zookeeper_znodeParent; }
    public String getHbaseMaster() { return this.hbase_master; }
    public String getHbaseRestPort() { return this.hbase_restPort; }

    public String getHiveHost() { return this.hive_host; }
    public String getHivePort() { return this.hive_port; }
    public String getHiveSuperUser() { return this.hive_superUser; }
    public String getHiveSuperUserKeytab() { return this.hive_superUserKeytab; }

    public String getAmbari_host(){return this.ambari_host;}
    public String getAmbari_adminUser(){return this.ambari_adminUser;}
    public String getAmbari_adminPwd(){return this.ambari_adminPwd;}

    public String getYarnRMHost(){return this.yarn_rm_host;}
    public String getYarnRMPort() { return this.yarn_rm_port; }
    public String getYarnRMUrl(){return this.yarn_rm_url;}
    public String getYarnSuperUser(){return this.yarn_superUser;}
    public String getYarnSuperUserKeytab(){return this.yarn_superUserKeytab;}

    public String getMRHistoryURL() { return this.mr_history_url; }

    public String getSparkHistoryURL() { return this.spark_history_url; }

    public etcdClient getEtcdClient(){
        return new etcdClient(this.etcd_host, this.etcd_port, this.etcd_user, this.etcd_pwd);
    }

    @Bean
    public LdapContextSource getLdapContextSource(){
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(this.ldap_url);
        contextSource.setUserDn(this.ldap_userDN);
        contextSource.setPassword(this.ldap_password);
        contextSource.setBase(this.ldap_base);
        return contextSource;
    }

    @Bean
    public LdapTemplate getLdapTemplate(){
        return new LdapTemplate(this.getLdapContextSource());
    }

    @Bean
    public rangerClient getRangerClient(){
        return new rangerClient(this.ranger_url, this.ranger_user, this.ranger_pwd);
    }

    @Bean
    public ambariClient getAmbariClient(){
        return new ambariClient(this.ambari_host,this.ambari_adminUser,this.ambari_adminPwd);
    }

    @Bean
    public yarnClient getYarnClient(){
        return new yarnClient(this.yarn_rm_url);
    }

}
