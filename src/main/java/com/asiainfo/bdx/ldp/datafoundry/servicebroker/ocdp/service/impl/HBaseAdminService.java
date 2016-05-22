package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import org.springframework.stereotype.Service;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.util.List;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HBaseAdminService implements OCDPAdminService{

    @Override
    public void authentication(){ System.out.println("HBase auth successful."); }

    @Override
    public String provisionResources(String serviceInstanceId, String bindingId){
        System.out.println("Create hbase folder successful.");
        String resourceName = "";
        return resourceName;
    }

    @Override
    public String assignPermissionToResources(String policyName, String resourceName, List<String> groupList,
                                              List<String> userList, List<String> permList){
        System.out.println("Assign read/write/execute permission to hbase table.");
        String rangerPolicyId = "";
        return rangerPolicyId;
    }

    @Override
    public boolean deprovisionResources(String serviceInstanceResuorceName){
        System.out.println("Delete hbase folder successful");
        return false;
    }

    @Override
    public void unassignPermissionFromResources(String policyId){
        System.out.println("Unassign read/write/execute permission to hbase table.");
    }

}
