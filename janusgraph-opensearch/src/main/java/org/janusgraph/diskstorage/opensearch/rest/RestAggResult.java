package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestAggResult {
    @JsonProperty("agg_result")
    private RestAggValue aggResult;

    public RestAggValue getAggResult() {
        return aggResult;
    }

    public void setAggResult(RestAggValue aggResult) {
        this.aggResult = aggResult;
    }
}

