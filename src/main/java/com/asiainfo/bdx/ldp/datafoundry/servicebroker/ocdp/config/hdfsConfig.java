package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Created by baikai on 5/18/16.
 */
@Configuration
public class hdfsConfig {

    @Value("${ocdp.hdfs.hdfsSuperUser}")
    private String hdfsSuperUser;

    @Value("${ocdp.hdfs.userKeytab}")
    private String userKeytab;

    @Value("${ocdp.hdfs.krbFilePath}")
    private String krbFilePath;

    @Value("${ocdp.hdfs.hdfsURL}")
    private String hdfsURL;

    @Bean
    public String getHdfsSuperUser(){ return this.hdfsSuperUser; }

    @Bean
    public String getUserKeytab() { return this.userKeytab; }

    @Bean
    public String getKrbFilePath() { return this.krbFilePath; }

    @Bean
    public String getHdfsURL() { return this.hdfsURL; }
}
