package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;

/**
 * Created by baikai on 8/12/16.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PlanMetadata {
    @JsonSerialize
    @JsonProperty("costs")
    private List<Costs> costs;

    @JsonSerialize
    @JsonProperty("bullets")
    List<Map<String, String>> bullets;

    @JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
    class Costs{
        @JsonSerialize
        @JsonProperty("amount")
        Map<String, Float> amount;

        @JsonSerialize
        @JsonProperty("unit")
        String unit;
    }

    public List<Costs> getCosts(){ return this.costs; }
    public List<Map<String, String>> getBullets() { return this.bullets; }
}
