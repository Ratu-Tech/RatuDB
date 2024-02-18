package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestAggResponse {
    private RestAggResult aggregations;

    public RestAggResult getAggregations() {
        return aggregations;
    }

    public void setAggregations(RestAggResult aggregations) {
        this.aggregations = aggregations;
    }
}
