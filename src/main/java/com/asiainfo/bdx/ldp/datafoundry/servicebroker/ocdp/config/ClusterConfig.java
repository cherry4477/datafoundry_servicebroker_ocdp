package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;
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
    private String etcd_endpoint;

    // LDAP connectivity properties
    private String ldap_url;

    private String ldap_userDN;

    private String ldap_password;

    private String ldap_base;

    // Kerberos connectivity properties
    private String krb_kdcHost;

    private String krb_userPrincipal;

    private String krb_keytabLocation;

    private String krb_adminPwd;

    private String krb_realm;

    // Hadoop Ranger connectivity properties
    private String ranger_uri;

    private String ranger_user;

    private String ranger_pwd;

    // Hadoop HDFS connectivity properties
    private String hdfs_url;

    private String hdfs_superUser;

    private String hdfs_userKeytab;

    private String hdfs_krbFilePath;

    @Override
    public void setEnvironment(Environment env){
        this.etcd_endpoint = env.getProperty("ETCD_ENDPOINT");
        this.ldap_url = env.getProperty("LDAP_URL");
        this.ldap_userDN = env.getProperty("LDAP_USER_DN");
        this.ldap_password = env.getProperty("LDAP_PASSWORD");
        this.ldap_base = env.getProperty("LDAP_BASE");
        this.krb_kdcHost = env.getProperty("KRB_KDC_HOST");
        this.krb_userPrincipal = env.getProperty("KRB_USER_PRINCIPAL");
        this.krb_keytabLocation = env.getProperty("KRB_KEYTAB_LOCATION");
        this.krb_adminPwd = env.getProperty("KRB_ADMIN_PASSWORD");
        this.krb_realm = env.getProperty("KRB_REALM");
        this.ranger_uri = env.getProperty("RANGER_URL");
        this.ranger_user = env.getProperty("RANGER_ADMIN_USER");
        this.ranger_pwd = env.getProperty("RANGER_ADMIN_PASSWORD");
        this.hdfs_url = env.getProperty("HDFS_URL");
        this.hdfs_superUser = env.getProperty("HDFS_SUPER_USER");
        this.hdfs_userKeytab = env.getProperty("HDFS_USER_KEYTAB");
        this.hdfs_krbFilePath = env.getProperty("HDFS_KRB_FILE");
    }

    public String getEtcdEndpoint() { return this.etcd_endpoint; }

    public String getLdapUrl() { return this.ldap_url; }
    public String getLdapUserDN() { return this.ldap_userDN; }
    public String getLdapPassword() { return this.ldap_password; }
    public String getLdapBase() { return this.ldap_base; }

    public String getKrbKdcHost() { return this.krb_kdcHost; }
    public String getKrbUserPrincipal() { return this.krb_userPrincipal; }
    public String getKrbKeytabLocation() { return this.krb_keytabLocation; }
    public String getKrbAdminPwd() { return this.krb_adminPwd; }
    public String getKrbRealm() { return this.krb_realm; }

    public String getRangerUri() { return this.ranger_uri; }
    public String getRangerUser() { return this.ranger_user; }
    public String getRangerPwd() { return this.ranger_pwd; }

    public String getHdfsUrl() { return this.hdfs_url;}
    public String getHdfsSuperUser() { return this.hdfs_superUser; }
    public String getHdfsUserKeytab() { return this.hdfs_userKeytab; }
    public String getHdfsKrbFilePath() {return this.hdfs_krbFilePath; }

    public etcdClient getEtcdClient(){
        return new etcdClient(this.etcd_endpoint);
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
        return new rangerClient(this.ranger_uri, this.ranger_user, this.ranger_pwd);
    }
}
