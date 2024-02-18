package org.janusgraph.diskstorage.opensearch;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.janusgraph.diskstorage.opensearch.OpenSearchConstants.ES_UPSERT_KEY;


public class OpenSearchMutation {

    public enum RequestType {

        INDEX,

        UPDATE,

        DELETE

    }

    private final RequestType requestType;

    private final String index;

    private final String type;

    private final String id;

    private final Map<String,Object> source;

    private OpenSearchMutation(RequestType requestType, String index, String type, String id, Map<String,Object> source) {
        this.requestType = requestType;
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    public static OpenSearchMutation createDeleteRequest(String index, String type, String id) {
        return new OpenSearchMutation(RequestType.DELETE, index, type, id, null);
    }

    public static OpenSearchMutation createIndexRequest(String index, String type, String id, Map<String,Object> source) {
        return new OpenSearchMutation(RequestType.INDEX, index, type, id, source);
    }

    public static OpenSearchMutation createUpdateRequest(String index, String type, String id, Map<String,Object> source) {
        return new OpenSearchMutation(RequestType.UPDATE, index, type, id, source);
    }

    public static OpenSearchMutation createUpdateRequest(String index, String type, String id, ImmutableMap.Builder<String,Object> builder, Map<String,Object> upsert) {
        final Map<String,Object> source = upsert == null ? builder.build() : builder.put(ES_UPSERT_KEY, upsert).build();
        return new OpenSearchMutation(RequestType.UPDATE, index, type, id, source);
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public Map<String,Object> getSource() {
        return source;
    }

}
