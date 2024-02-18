package org.janusgraph.diskstorage.opensearch.mapping;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TypedIndexMappings {

    private Map<String, IndexMapping> mappings;

    public Map<String, IndexMapping> getMappings() {
        return mappings;
    }

    public void setMappings(Map<String, IndexMapping> mappings){
        this.mappings = mappings;
    }

}
