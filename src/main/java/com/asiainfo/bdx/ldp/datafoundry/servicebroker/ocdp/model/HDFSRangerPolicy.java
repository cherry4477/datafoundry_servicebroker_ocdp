package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by baikai on 6/13/16.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class HDFSRangerPolicy extends BaseRangerPolicy {

    @JsonSerialize
    @JsonProperty("resourcePath")
    private String resourcePath;

    public HDFSRangerPolicy(String policyName, String id, String resourcePath, String description,
                            String repositoryName, String repositoryType, boolean isEnabled,
                            boolean isRecursive, boolean isAuditEnabled){
        super(policyName, id, description, repositoryName, repositoryType, isEnabled, isRecursive, isAuditEnabled);
        this.resourcePath = resourcePath;
    }

    public String getResourcePath() { return this.resourcePath; }
}
