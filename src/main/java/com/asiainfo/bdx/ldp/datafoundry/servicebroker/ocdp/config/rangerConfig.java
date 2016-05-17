package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.rangerClient;

import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Created by baikai on 5/16/16.
 */
@Configuration
public class rangerConfig {

    @Value("${ocdp.ranger.rangerUri}")
    private String rangerUri;

    @Value("${ocdp.ranger.rangerUser}")
    private String rangerUser;

    @Value("${ocdp.ranger.rangerPwd}")
    private String rangerPwd;

    @Bean
    public rangerClient getRangerClient(){
        return new rangerClient(this.rangerUri, this.rangerUser, this.rangerPwd);
    }
}
