package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Apache Ranger Policy Definition.
 * Created by baikai on 5/17/16.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RangerPolicy {
    @JsonSerialize
    @JsonProperty("policyName")
    private String policyName;

    @JsonSerialize
    @JsonProperty("id")
    private String id;

    @JsonSerialize
    @JsonProperty("resourceName")
    private String resourceName;

    @JsonSerialize
    @JsonProperty("description")
    private String description;

    @JsonSerialize
    @JsonProperty("repositoryName")
    private String repositoryName;

    @JsonSerialize
    @JsonProperty("repositoryType")
    private String repositoryType;

    @JsonSerialize
    @JsonProperty("isEnabled")
    private boolean isEnabled;

    @JsonSerialize
    @JsonProperty("isRecursive")
    private boolean isRecursive;

    @JsonSerialize
    @JsonProperty("isAuditEnabled")
    private boolean isAuditEnabled;

    private class Permission{
        private List<String> permList = new ArrayList<String>();
        private List<String> userList = new ArrayList<String>();
        private List<String> groupList = new ArrayList<String>();
    }

    @JsonSerialize
    @JsonProperty("permMapList")
    private List<Permission> permMapList;

    public RangerPolicy(String policyName, String id, String resourceName, String description,
                        String repositoryName, String repositoryType, boolean isEnabled,
                        boolean isRecursive, boolean isAuditEnabled){
        this.policyName = policyName;
        this.id = id;
        this.resourceName = resourceName;
        this.description = description;
        this.repositoryName = repositoryName;
        this.repositoryType = repositoryType;
        this.isEnabled = isEnabled;
        this.isAuditEnabled = isAuditEnabled;
        this.isRecursive = isRecursive;
        this.permMapList = new ArrayList<Permission>();
    }

    public void addPermToPolicy(List<String> groupList, List<String> userList, List<String> permList){
        Permission p = new Permission();
        p.groupList.addAll(groupList);
        p.userList.addAll(userList);
        p.permList.addAll(permList);
        this.permMapList.add(p);
    }

    public String getPolicyName(){ return policyName; }
    public String getPolicyId() { return id; }
    public String getResourceName(){ return resourceName; }
    public String getDescription(){ return description; }
    public String getRepoName(){ return repositoryName; }
    public String getRepoType(){ return repositoryType; }
    public List<Permission> getPermMapList(){ return permMapList; }
}
