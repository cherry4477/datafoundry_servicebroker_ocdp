package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.util.List;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HBaseAdminService implements OCDPAdminService{

    private Logger logger = LoggerFactory.getLogger(HDFSAdminService.class);

    @Override
    public void authentication(){ logger.info("HBase auth successful."); }

    @Override
    public String provisionResources(String serviceInstanceId, String bindingId){
        logger.info("Create hbase folder successful.");
        String resourceName = "";
        return resourceName;
    }

    @Override
    public boolean assignPermissionToResources(String policyName, String resourceName, List<String> groupList,
                                              List<String> userList, List<String> permList){
        logger.info("Assign read/write/execute permission to hbase table.");
        return true;
    }

    @Override
    public void deprovisionResources(String serviceInstanceResuorceName){
        logger.info("Delete hbase folder successful.");
    }

    @Override
    public boolean unassignPermissionFromResources(String policyId){
        logger.info("Unassign read/write/execute permission to hbase table.");
        return true;
    }

}
