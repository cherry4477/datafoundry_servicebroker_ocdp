package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;

import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ldapConfig {

    @Value("${ocdp.ldap.url}")
    private String url;

    @Value("${ocdp.ldap.userDN}")
    private String userDN;

    @Value("${ocdp.ldap.password}")
    private String password;

    @Value("${ocdp.ldap.base}")
    private String base;

    @Bean
    public LdapContextSource getLdapContextSource(){
        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(this.url);
        contextSource.setUserDn(this.userDN);
        contextSource.setPassword(this.password);
        contextSource.setBase(this.base);
        return contextSource;
    }

    @Bean
    public LdapTemplate getLdapTemplate(){
        return new LdapTemplate(this.getLdapContextSource());
    }
}
