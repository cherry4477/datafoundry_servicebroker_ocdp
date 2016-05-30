package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.io.IOException;
import java.util.Map;
import java.util.List;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPAdminService {

    void authentication() throws Exception;

	String provisionResources(String serviceInstanceId, String bindingId) throws Exception;

    boolean assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName);

    void deprovisionResources(String serviceInstanceResuorceName) throws Exception;

    boolean unassignPermissionFromResources(String policyId);

}
