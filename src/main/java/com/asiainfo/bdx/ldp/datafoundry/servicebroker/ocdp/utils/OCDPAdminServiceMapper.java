package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by baikai on 5/19/16.
 */
public class OCDPAdminServiceMapper {

    private static final Map<String, String> OCDP_ADMIN_SERVICE_MAP = new HashMap<String, String>(){
        {
            put("ae67d4ba-5c4e-4937-a68b-5b47cfe356d8", "HDFSAdminService");
            put("d9845ade-9410-4c7f-8689-4e032c1a8450", "HBaseAdminService");
            put("2ef26018-003d-4b2b-b786-0481d4ee9fa3", "hiveAdminService");
        }
    };

    public static String getOCDPAdminService(String serviceDefinitionId){
        return OCDP_ADMIN_SERVICE_MAP.get(serviceDefinitionId);
    }
}
