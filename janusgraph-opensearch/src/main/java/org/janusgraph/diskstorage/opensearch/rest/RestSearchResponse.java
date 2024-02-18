package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.diskstorage.opensearch.OpenSearchResponse;

import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestSearchResponse extends OpenSearchResponse {

    private long took;

    @JsonProperty("hits")
    private RestSearchResults hits;

    @JsonProperty("_scroll_id")
    private String scrollId;

    @Override
    public long getTook() {
        return took;
    }

    @Override
    public void setTook(long took) {
        this.took = took;
    }

    public RestSearchResults getHits() {
        return hits;
    }

    public void setHits(RestSearchResults hits) {
        this.hits = hits;
    }

    public int getNumHits() {
        return hits.getHits().size();
    }

    public Float getMaxScore() {
        return hits.getMaxScore();
    }

    @Override
    public Stream<RawQuery.Result<String>> getResults() {
        return hits.getHits().stream()
            .map(hit -> new RawQuery.Result<>(hit.getId(), hit.getScore() != null ? hit.getScore() : 0f));
    }

    @Override
    public int numResults() {
        return hits.getHits().size();
    }

    @Override
    public String getScrollId() {
        return scrollId;
    }

    @Override
    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }
}
