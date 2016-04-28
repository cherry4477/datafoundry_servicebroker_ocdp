package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.cloud.servicebroker.model.Catalog;
import org.springframework.cloud.servicebroker.model.Plan;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CatalogConfig {
	
	@Bean
	public Catalog catalog() {
		return new Catalog(Collections.singletonList(
				new ServiceDefinition(
						"datafoundry-ocdp-service-broker",
						"OCDP",
						"A OCDP Hadoop service broker implementation",
						true,
						false,
						Collections.singletonList(
								new Plan("ocdp-plan",
										"Default ocdp Plan",
										"This is a default ocdp plan.  All services are created equally.",
										getPlanMetadata())),
						Arrays.asList("ocdp", "document"),
						getServiceDefinitionMetadata(),
						null,
						null)));
	}
	
/* Used by Pivotal CF console */

	private Map<String, Object> getServiceDefinitionMetadata() {
		Map<String, Object> sdMetadata = new HashMap<>();
		sdMetadata.put("displayName", "OCDP");
		sdMetadata.put("imageUrl", "http://...");
		sdMetadata.put("longDescription", "OCDP Service");
		sdMetadata.put("providerDisplayName", "AsiaInfo");
		sdMetadata.put("documentationUrl", "https://github.com/baikai/datafoundry_servicebroker_ocdp");
		sdMetadata.put("supportUrl", "https://github.com/baikai/datafoundry_servicebroker_ocdp");
		return sdMetadata;
	}
	
	private Map<String,Object> getPlanMetadata() {
		Map<String,Object> planMetadata = new HashMap<>();
		planMetadata.put("costs", getCosts());
		planMetadata.put("bullets", getBullets());
		return planMetadata;
	}

	private List<Map<String,Object>> getCosts() {
		Map<String,Object> costsMap = new HashMap<>();
		
		Map<String,Object> amount = new HashMap<>();
		amount.put("usd", 0.0);
	
		costsMap.put("amount", amount);
		costsMap.put("unit", "MONTHLY");
		
		return Collections.singletonList(costsMap);
	}
	
	private List<String> getBullets() {
		return Arrays.asList("Shared OCDP server",
				"20 GB Storage (not enforced)",
				"10 concurrent connections (not enforced)");
	}
	
}