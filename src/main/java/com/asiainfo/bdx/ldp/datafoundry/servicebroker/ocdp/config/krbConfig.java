package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class krbConfig {

    @Value("${ocdp.krb.user-principal}")
    private String userPrincipal;

    @Value("${ocdp.krb.keytab-location}")
    private String keytabLocation;

    @Value("${ocdp.krb.adminPwd}")
    private String adminPwd;

    @Value("${ocdp.krb.kdcHost}")
    private String kdcHost;

    @Value("${ocdp.krb.realm}")
    private String realm;

    @Bean
    public String getUserPrincipal(){
        return this.userPrincipal;
    }

    @Bean
    public String getKeytabLocation(){
        return this.keytabLocation;
    }

    @Bean
    public String getAdminPwd(){
        return this.adminPwd;
    }

    @Bean
    public String getKdcHost(){
        return this.kdcHost;
    }

    @Bean
    public String getRealm(){
        return this.realm;
    }
}
