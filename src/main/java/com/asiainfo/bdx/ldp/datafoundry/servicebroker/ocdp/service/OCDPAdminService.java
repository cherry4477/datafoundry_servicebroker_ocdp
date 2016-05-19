package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import java.util.Map;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public interface OCDPAdminService {

    void authentication();

	boolean provisionResources();

    void assignPermissionToResources();

    boolean deprovisionResources();

    void unassignPermissionFromResources();

    Map<String, Object> generateCredentials();
}
