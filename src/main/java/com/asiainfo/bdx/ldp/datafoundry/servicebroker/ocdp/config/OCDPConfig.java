package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OCDPConfig {

	@Value("${ocdp.host:localhost}")
	private String host;

	@Value("${ocdp.port:27017}")
	private int port;

}
