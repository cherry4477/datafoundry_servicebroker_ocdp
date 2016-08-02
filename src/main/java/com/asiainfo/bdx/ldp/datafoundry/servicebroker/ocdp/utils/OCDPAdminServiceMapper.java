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
            put("ae0f2324-27a8-415b-9c7f-64ab6cd88d40", "yarnAdminService");
        }
    };

    private static final Map<String, String> OCDP_SERVICE_PLAN_MAP = new HashMap<String, String>(){
        {
            put("ae67d4ba-5c4e-4937-a68b-5b47cfe356d8", "72150b09-1025-4533-8bae-0e04ef68ac13");
            put("d9845ade-9410-4c7f-8689-4e032c1a8450", "f658e391-b7d6-4b72-9e4c-c754e4943ae1");
            put("2ef26018-003d-4b2b-b786-0481d4ee9fa3", "aa7e364f-fdbf-4187-b60a-218b6fa398ed");
            put("ae0f2324-27a8-415b-9c7f-64ab6cd88d40", "6524c793-0ea5-4456-9a60-ca70271decdc");
        }
    };

    public static String getOCDPAdminService(String serviceDefinitionId){
        return OCDP_ADMIN_SERVICE_MAP.get(serviceDefinitionId);
    }

    public static String getOCDPServicePlan(String serviceDefinitionId){
        return OCDP_SERVICE_PLAN_MAP.get(serviceDefinitionId);
    }
}
