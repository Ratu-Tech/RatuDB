package org.janusgraph.diskstorage.opensearch.mapping;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TypelessIndexMappings {

    private IndexMapping mappings;

    public IndexMapping getMappings() {
        return mappings;
    }

    public void setMappings(IndexMapping mappings) {
        this.mappings = mappings;
    }
}
