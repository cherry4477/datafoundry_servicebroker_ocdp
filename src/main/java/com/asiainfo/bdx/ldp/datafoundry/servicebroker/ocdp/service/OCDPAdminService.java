package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.Map;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPAdminService {

	String provisionResources(String serviceDefinitionId, String planId, String serviceInstanceId,
                              String bindingId) throws Exception;

    String assignPermissionToResources(String policyName, String resourceName, String accountName, String groupName);

    boolean appendUserToResourcePermission(String policyId, String groupName, String accountName);

    void deprovisionResources(String serviceInstanceResuorceName) throws Exception;

    boolean unassignPermissionFromResources(String policyId);

    boolean removeUserFromResourcePermission(String policyId, String groupName, String accountName);

    String getDashboardUrl();

    Map<String, Object> generateCredentialsInfo(String accountName, String accountPwd, String accountKeytab,
                                                String serviceInstanceResource, String rangerPolicyId);

}
