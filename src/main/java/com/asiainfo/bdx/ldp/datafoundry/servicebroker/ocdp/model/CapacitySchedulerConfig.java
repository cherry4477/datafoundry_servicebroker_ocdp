package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.security.auth.login.ConfigurationSpi;

/**
 * Created by Aaron on 16/7/20.
 */
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CapacitySchedulerConfig {

    @JsonSerialize
    @JsonProperty("items")
    private List<Configs> items;

    public class Configs{

        private String tag;

        private String type;

        private String version;

        private Map<String,String> Config;

        private Map<String,String> properties;

        public String getTag(){return tag;}
        public String getType(){return type;}
        public String getVersion(){return version;}
        public Map<String,String> getConfig(){return Config;}
        public Map<String,String> getProperties(){return properties;}

    }

    public CapacitySchedulerConfig()
    {
        this.items = new ArrayList<Configs>();
    }

    public List<Configs> getItems(){
        return this.items;
    }




}
