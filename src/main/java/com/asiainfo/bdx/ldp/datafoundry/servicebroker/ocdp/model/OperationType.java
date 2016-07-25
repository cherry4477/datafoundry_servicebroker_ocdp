package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.model;

/**
 * Created by baikai on 7/25/16.
 */
public enum OperationType {
    /**
     * Indicates that a request is still being processed. Cloud Foundry will continue polling for the current state.
     */
    PROVISION("provision"),

    /**
     * Indicates that a request completed successfully. Cloud Foundry will stop polling for the current state.
     */
    DELETE("delete");

    private final String state;

    OperationType(String state) {
        this.state = state;
    }

    public String getValue() {
        return state;
    }
}