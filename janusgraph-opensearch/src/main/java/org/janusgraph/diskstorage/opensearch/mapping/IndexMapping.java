package org.janusgraph.diskstorage.opensearch.mapping;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexMapping {

    private Map<String, Object> properties;

    private String dynamic;

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public boolean isDynamic() {
        return dynamic == null || "true".equalsIgnoreCase(dynamic);
    }

    public void setDynamic(String dynamic) {
        this.dynamic = dynamic;
    }
}
