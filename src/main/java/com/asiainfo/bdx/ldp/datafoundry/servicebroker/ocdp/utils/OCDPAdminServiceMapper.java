package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
public class OCDPAdminServiceMapper {

    private static final Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>(){
        {
            put("hdfs-shared", "HDFSAdminService");
            put("hbase-shared", "HBaseAdminService");
            put("hive-shared", "HiveAdminService");
        }
    };

    public static String getOCDPAdminService(String serviceDefinitionId){
        return OCDP_ADMIN_SERVICE_MAP.get(serviceDefinitionId);
    }
}
