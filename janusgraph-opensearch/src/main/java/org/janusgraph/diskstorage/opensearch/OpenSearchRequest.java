package org.janusgraph.diskstorage.opensearch;

import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpenSearchRequest {

    private Map<String,Object> query;

    private Integer size;

    private Integer from;

    private final List<Map<String,RestSortInfo>> sorts;

    private List<String> fields;

    private boolean disableSourceRetrieval;

    public OpenSearchRequest() {
        this.sorts = new ArrayList<>();
        this.fields = new ArrayList<>();
    }

    public Map<String,Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String,Object> query) {
        this.query = query;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public List<Map<String,RestSortInfo>> getSorts() {
        return sorts;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public void addSort(String key, String order, String unmappedType) {
        this.sorts.add(ImmutableMap.of(key, new RestSortInfo(order, unmappedType)));
    }

    public boolean isDisableSourceRetrieval() {
        return disableSourceRetrieval;
    }

    public void setDisableSourceRetrieval(boolean disableSourceRetrieval) {
        this.disableSourceRetrieval = disableSourceRetrieval;
    }

    public static class RestSortInfo {

        String order;

        @JsonProperty("unmapped_type")
        String unmappedType;

        public RestSortInfo(String order, String unmappedType) {
            this.order = order;
            this.unmappedType = unmappedType;
        }

        public String getOrder() {
            return order;
        }

        public void setOrder(String order) {
            this.order = order;
        }

        public String getUnmappedType() {
            return unmappedType;
        }

        public void setUnmappedType(String unmappedType) {
            this.unmappedType = unmappedType;
        }

    }
}
