package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/3/21.
 * V2 API is compatible with Apache Ranger 5.0 +.
 * OCDP Service broker will only use V2 API.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RangerV2Policy{

    @JsonSerialize
    @JsonProperty("id")
    private String id;

    @JsonSerialize
    @JsonProperty("isEnabled")
    private boolean isEnabled;

    @JsonSerialize
    @JsonProperty("isAuditEnabled")
    private boolean isAuditEnabled;

    @JsonSerialize
    @JsonProperty("service")
    private String service;

    @JsonSerialize
    @JsonProperty("name")
    private String name;

    @JsonSerialize
    @JsonProperty("description")
    private String description;

    @JsonSerialize
    @JsonProperty("resources")
    private Map<String, RangerResource> resources;

    @JsonSerialize
    @JsonProperty("policyItems")
    private List<PolicyItem> policyItems;

    public RangerV2Policy(String policyName, String id, String description,
                            String service, boolean isEnabled, boolean isAuditEnabled){
        this.id = id;
        this.name = policyName;
        this.description = description;
        this.service = service;
        this.isAuditEnabled = isAuditEnabled;
        this.isEnabled = isEnabled;
        this.resources = new HashMap<>();
        this.policyItems = new ArrayList<>();
    }

    public void addResources(String resourceType, List<String> resourceList, boolean isExcludes){
        RangerResource rr = new RangerResource();
        rr.values.addAll(resourceList);
        rr.isExcludes = isExcludes;
        resources.put(resourceType, rr);
    }

    public void addResources2(String resourceType, List<String> resourceList, boolean isExcludes, boolean isRecursive){
        RangerResource2 rr = new RangerResource2();
        rr.values.addAll(resourceList);
        rr.isExcludes = isExcludes;
        rr.isRecursive = isRecursive;
        resources.put(resourceType, rr);
    }

    public void addPolicyItems(List<String> users, List<String> groups, List<String> conditions,
                               boolean delegateAdmin, List<String> types){
        PolicyItem pi = new PolicyItem();
        pi.delegateAdmin = delegateAdmin;
        pi.users.addAll(users);
        pi.groups.addAll(groups);
        pi.conditions.addAll(conditions);
        pi.accesses = pi.getAccesses(types);

        policyItems.add(pi);
    }

    public void updatePolicy(String groupName, String accountName, List<String> accessTypes, boolean isAppend){

        this.policyItems.get(0).groups.clear();
        this.policyItems.get(0).groups.add(groupName);

        if(isAppend){
            this.policyItems.get(0).users.add(accountName);
        }else{
            this.policyItems.get(0).users.remove(accountName);
        }

        this.policyItems.get(0).accesses.clear();
        this.policyItems.get(0).accesses = new PolicyItem().getAccesses(accessTypes);

    }

    public String getPolicyId(){return id;}
    public List<PolicyItem> getPolicyItems(){return policyItems;}
    public List<String> getUserList(){
        return this.policyItems.get(0).getUsers();
    }
    public List<String> getGroupList(){
        return this.policyItems.get(0).getGroups();
    }
    public List<String> getResourceValues(){
        return this.resources.get("queue").values;
    }

    class RangerResource2 extends RangerResource{
        boolean isRecursive;
    }

    class RangerResource{
        boolean isExcludes;
        List<String> values = new ArrayList<String>();
    }

    class PolicyItem{
        List<String> users = new ArrayList<String>();
        List<String> groups = new ArrayList<String>();
        boolean delegateAdmin;
        List<RangerAccess> accesses = new ArrayList<RangerAccess>();
        List<String> conditions = new ArrayList<String>();

        private class RangerAccess {
            boolean isAllowed;
            String type;

            RangerAccess(boolean isAllowed, String type){
                this.isAllowed = isAllowed;
                this.type = type;
            }
        }

        public List<RangerAccess> getAccesses(List<String> types){

            List<RangerAccess> accesses = new ArrayList<RangerAccess>();
            int elementNum = 0;
            while(elementNum < types.size()){
                accesses.add(new RangerAccess(true,types.get(elementNum)));
                elementNum++;
            }
            return accesses;
        }

        public List<String> getUsers(){
            return users;
        }

        public List<String> getGroups(){
            return groups;
        }
    }

}
