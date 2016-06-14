package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Created by baikai on 6/13/16.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class HBaseRangerPolicy extends BaseRangerPolicy{

    @JsonSerialize
    @JsonProperty("resourcePath")
    private String tables;

    @JsonSerialize
    @JsonProperty("resourcePath")
    private String columnFamilies;

    @JsonSerialize
    @JsonProperty("resourcePath")
    private String columns;

    public HBaseRangerPolicy(String policyName, String id, String tables, String columnFamilies, String columns,
                             String description, String repositoryName, String repositoryType, boolean isEnabled,
                             boolean isRecursive, boolean isAuditEnabled){
        super(policyName, id, description, repositoryName, repositoryType, isEnabled, isRecursive, isAuditEnabled);
        this.tables = tables;
        this.columnFamilies = columnFamilies;
        this.columns = columns;
    }

    public String getTables() { return this.tables; }
    public String getColumnFamilies() { return this.columnFamilies; }
    public String getColumns() { return this.columns; }
}
