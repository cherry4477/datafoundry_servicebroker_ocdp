package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by baikai on 6/13/16.
 * V1 API is compatible with Apache Ranger 4.x or before.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class HDFSRangerV1Policy extends BaseRangerV1Policy {

    @JsonSerialize
    @JsonProperty("resourceName")
    private String resourceName;

    public HDFSRangerV1Policy(String policyName, String id, String resourcePath, String description,
                              String repositoryName, String repositoryType, boolean isEnabled,
                              boolean isRecursive, boolean isAuditEnabled){
        super(policyName, id, description, repositoryName, repositoryType, isEnabled, isRecursive, isAuditEnabled);
        this.resourceName = resourcePath;
    }

    public String getResourcePath() { return this.resourceName; }
}
