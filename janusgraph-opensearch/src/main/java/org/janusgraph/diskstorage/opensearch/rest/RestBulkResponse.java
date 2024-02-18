package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
public class RestBulkResponse {

    private boolean errors;

    private List<Map<String, RestBulkItemResponse>> items;

    public boolean isErrors() {
        return errors;
    }

    public void setErrors(boolean errors) {
        this.errors = errors;
    }

    public List<Map<String, RestBulkItemResponse>> getItems() {
        return items;
    }

    public void setItems(List<Map<String, RestBulkItemResponse>> items) {
        this.items = items;
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    public static class RestBulkItemResponse {

        private String result;

        private int status;

        private Object error;

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public Object getError() {
            return error;
        }

        public void setError(Object error) {
            this.error = error;
        }
    }

}
