package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.client.etcdClient;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EtcdConfig {

	@Value("${ocdp.etcd.endpoint: http://127.0.0.1:2379}")
	private String endpoint;

    public etcdClient getEtcdClient(){
        return new etcdClient(this.endpoint);
    }
}
