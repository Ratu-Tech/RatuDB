package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestSearchResults {

    @JsonProperty("max_score")
    private Float maxScore;

    private List<RestSearchHit> hits;

    public Float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(Float maxScore) {
        this.maxScore = maxScore;
    }

    public List<RestSearchHit> getHits() {
        return hits;
    }

    public void setHits(List<RestSearchHit> hits) {
        this.hits = hits;
    }

}
