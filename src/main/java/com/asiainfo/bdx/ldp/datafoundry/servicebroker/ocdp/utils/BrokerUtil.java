package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by baikai on 10/17/16.
 */
public class BrokerUtil {

    public static void authentication(Configuration conf, String userPrincipal, String keyTabFilePath){
        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(userPrincipal, keyTabFilePath);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
