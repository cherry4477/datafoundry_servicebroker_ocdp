package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by baikai on 6/14/16.
 * V1 API is compatible with Apache Ranger 4.x or before.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class HiveRangerV1Policy extends BaseRangerV1Policy {

    @JsonSerialize
    @JsonProperty("databases")
    private String databases;

    @JsonSerialize
    @JsonProperty("table")
    private String tables;

    @JsonSerialize
    @JsonProperty("column")
    private String columns;

    public HiveRangerV1Policy(String policyName, String id, String databases, String tables, String columns,
                              String description, String repositoryName, String repositoryType, boolean isEnabled,
                              boolean isRecursive, boolean isAuditEnabled){
        super(policyName, id, description, repositoryName, repositoryType, isEnabled, isRecursive, isAuditEnabled);
        this.databases = databases;
        this.tables = tables;
        this.columns = columns;
    }

    public String getDatabases() { return this.databases; }
    public String getTables() { return this.tables; }
    public String getColumns() { return  this.columns; }

}
