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

    void authentication()throws IOException;

	String provisionResources(String serviceInstanceId);

    String assignPermissionToResources(String policyName, String resourceName, List<String> groupList,
                                       List<String> userList, List<String> permList);

    boolean deprovisionResources(String serviceInstanceResuorceName);

    void unassignPermissionFromResources(String policyId);

}
