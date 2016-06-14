package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by baikai on 6/13/16.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BaseRangerPolicy {

    @JsonSerialize
    @JsonProperty("policyName")
    private String policyName;

    @JsonSerialize
    @JsonProperty("id")
    private String id;

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

    @JsonSerialize
    @JsonProperty("permMapList")
    private List<Permission> permMapList;

    private class Permission{
        private List<String> permList = new ArrayList<String>();
        private List<String> userList = new ArrayList<String>();
        private List<String> groupList = new ArrayList<String>();
    }

    public BaseRangerPolicy(String policyName, String id, String description,
                            String repositoryName, String repositoryType, boolean isEnabled,
                            boolean isRecursive, boolean isAuditEnabled){
        this.policyName = policyName;
        this.id = id;
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

    public void updatePolicyPerm(String groupName, String accountName, List<String> permList, boolean isAppend){
        // Should reconstruct permMapList here, because ranger returned policy definition like
        // "permMapList":[{"permList":["Read","Write","Create","Admin"],"userList":["xxx"],"groupList":[]},
        // {"permList":["Read","Write","Create","Admin"],"userList":[],"groupList":["public"]}]", this format
        // cannot use to update policy by call ranger rest api.
        this.permMapList.remove(1);
        this.permMapList.get(0).groupList.add(groupName);
        if(isAppend){
            this.permMapList.get(0).userList.add(accountName);
        }else{
            this.permMapList.get(0).userList.remove(accountName);
        }
        this.permMapList.get(0).permList.clear();
        this.permMapList.get(0).permList.addAll(permList);
    }

    public String getPolicyName(){ return policyName; }
    public String getPolicyId() { return id; }
    public String getDescription(){ return description; }
    public String getRepoName(){ return repositoryName; }
    public String getRepoType(){ return repositoryType; }
    public List<Permission> getPermMapList(){ return permMapList; }

}
