package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
public class OCDPAdminServiceMapper {

    private static final Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>(){
        {
            put("datafoundry-hdfs-service-broker", "HDFSAdminService");
            put("datafoundry-hbase-service-broker", "HBaseAdminService");
            put("datafoundry-hive-service-broker", "HiveAdminService");
        }
    };

    public static String getOCDPAdminService(String serviceID){
        return OCDP_ADMIN_SERVICE_MAP.get(serviceID);
    }
}
