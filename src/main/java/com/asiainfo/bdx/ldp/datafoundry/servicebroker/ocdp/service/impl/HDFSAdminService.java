package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.impl;

import org.springframework.stereotype.Service;
import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service.OCDPAdminService;

import java.util.Collections;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
@Service
public class HDFSAdminService implements OCDPAdminService{

    @Override
    public void authentication(){ System.out.println("HDFS auth successful."); }

    @Override
    public boolean provisionResources(){
        System.out.println("Create hdfs folder successful.");
        return true;
    }

    @Override
    public void assignPermissionToResources(){
        System.out.println("Assign read/write/execute permission to hdfs folder.");
    }

    @Override
    public boolean deprovisionResources(){
        System.out.println("Delete hdfs folder successful");
        return false;
    }

    @Override
    public void unassignPermissionFromResources(){
        System.out.println("Unassign read/write/execute permission to hdfs folder.");
    }

    @Override
    public Map<String, Object> generateCredentials(){
        return Collections.singletonMap("uri", new Object());
    }

}
