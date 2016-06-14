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

	String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                              String bindingId) throws Exception;

    String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName);

    boolean appendUserToResourcePermission(String policyId, String groupName, String accountName);

    void deprovisionResources(String serviceInstanceResuorceName) throws Exception;

    boolean unassignPermissionFromResources(String policyId);

    boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName);

    String getDashboardUrl();

}
