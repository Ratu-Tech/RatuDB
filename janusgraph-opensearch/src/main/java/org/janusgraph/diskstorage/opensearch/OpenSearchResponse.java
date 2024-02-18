package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.indexing.RawQuery;

import java.util.List;
import java.util.stream.Stream;

public class OpenSearchResponse {

    private long took;

    private String scrollId;

    private List<RawQuery.Result<String>> results;

    public long getTook() {
        return took;
    }

    public void setTook(long took) {
        this.took = took;
    }

    public Stream<RawQuery.Result<String>> getResults() {
        return results.stream();
    }

    public void setResults(List<RawQuery.Result<String>> results) {
        this.results = results;
    }

    public int numResults() {
        return results.size();
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }
}
