
package org.janusgraph.diskstorage.opensearch.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;
import org.apache.tinkerpop.shaded.jackson.core.JsonParseException;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectReader;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectWriter;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.opensearch.OpenMajorVersion;
import org.janusgraph.diskstorage.opensearch.OpenSearchClient;
import org.janusgraph.diskstorage.opensearch.OpenSearchMutation;
import org.janusgraph.diskstorage.opensearch.mapping.IndexMapping;
import org.janusgraph.diskstorage.opensearch.mapping.TypedIndexMappings;
import org.janusgraph.diskstorage.opensearch.mapping.TypelessIndexMappings;
import org.janusgraph.diskstorage.opensearch.script.ESScriptResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;

public class RestOpenSearchClient implements OpenSearchClient {

    private static final Logger log = LoggerFactory.getLogger(RestOpenSearchClient.class);

    private static final String REQUEST_TYPE_DELETE = "DELETE";
    private static final String REQUEST_TYPE_GET = "GET";
    private static final String REQUEST_TYPE_POST = "POST";
    private static final String REQUEST_TYPE_PUT = "PUT";
    private static final String REQUEST_TYPE_HEAD = "HEAD";
    private static final String REQUEST_SEPARATOR = "/";
    private static final String REQUEST_PARAM_BEGINNING = "?";
    private static final String REQUEST_PARAM_SEPARATOR = "&";

    public static final String INCLUDE_TYPE_NAME_PARAMETER = "include_type_name";

    private static final byte[] NEW_LINE_BYTES = "\n".getBytes(UTF8_CHARSET);

    private static final Request INFO_REQUEST = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR);

    private static final ObjectMapper mapper;
    private static final ObjectReader mapReader;
    private static final ObjectWriter mapWriter;

    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(new Geoshape.GeoshapeGsonSerializerV2d0());
        mapper = new ObjectMapper();
        mapper.registerModule(module);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapReader = mapper.readerWithView(Map.class).forType(HashMap.class);
        mapWriter = mapper.writerWithView(Map.class);
    }

    private static final OpenMajorVersion DEFAULT_VERSION = OpenMajorVersion.EIGHT;

    private static final Function<StringBuilder, StringBuilder> APPEND_OP = sb -> sb.append(sb.length() == 0 ? REQUEST_PARAM_BEGINNING : REQUEST_PARAM_SEPARATOR);

    private final RestClient delegate;

    private OpenMajorVersion majorVersion;

    private String bulkRefresh;

    private boolean bulkRefreshEnabled = false;

    private final String scrollKeepAlive;

    private final boolean useMappingTypes;

    private final boolean esVersion7;

    private Integer retryOnConflict;

    private final String retryOnConflictKey;

    public RestOpenSearchClient(RestClient delegate, int scrollKeepAlive, boolean useMappingTypesForES7) {
        this.delegate = delegate;
        majorVersion = getMajorVersion();
        this.scrollKeepAlive = scrollKeepAlive + "s";
        esVersion7 = OpenMajorVersion.SEVEN.equals(majorVersion);
        useMappingTypes = majorVersion.getValue() < 7 || (useMappingTypesForES7 && esVersion7);
        retryOnConflictKey = majorVersion.getValue() >= 7 ? "retry_on_conflict" : "_retry_on_conflict";
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public OpenMajorVersion getMajorVersion() {
        if (majorVersion != null) {
            return majorVersion;
        }

        majorVersion = DEFAULT_VERSION;
        try {
            final Response response = delegate.performRequest(INFO_REQUEST);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final ClusterInfo info = mapper.readValue(inputStream, ClusterInfo.class);
                majorVersion = OpenMajorVersion.parse(info.getVersion() != null ? (String) info.getVersion().get("number") : null);
            }
        } catch (final IOException e) {
            log.warn("Unable to determine Elasticsearch server version. Default to {}.", majorVersion, e);
        }

        return majorVersion;
    }

    @Override
    public void clusterHealthRequest(String timeout) throws IOException {
        Request clusterHealthRequest = new Request(REQUEST_TYPE_GET,
            REQUEST_SEPARATOR + "_cluster" + REQUEST_SEPARATOR + "health");
        clusterHealthRequest.addParameter("wait_for_status", "yellow");
        clusterHealthRequest.addParameter("timeout", timeout);

        final Response response = delegate.performRequest(clusterHealthRequest);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final Map<String, Object> values = mapReader.readValue(inputStream);
            if (!values.containsKey("timed_out")) {
                throw new IOException("Unexpected response for Elasticsearch cluster health request");
            } else if (!Objects.equals(values.get("timed_out"), false)) {
                throw new IOException("Elasticsearch timeout waiting for yellow status");
            }
        }
    }

    @Override
    public boolean indexExists(String indexName) throws IOException {
        final Response response = delegate.performRequest(new Request(REQUEST_TYPE_HEAD, REQUEST_SEPARATOR + indexName));
        return response.getStatusLine().getStatusCode() == 200;
    }

    @Override
    public boolean isIndex(String indexName) {
        try {
            final Response response = delegate.performRequest(new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName));
            try (final InputStream inputStream = response.getEntity().getContent()) {
                return mapper.readValue(inputStream, Map.class).containsKey(indexName);
            }
        } catch (final IOException ignored) {
        }
        return false;
    }

    @Override
    public boolean isAlias(String aliasName) {
        try {
            delegate.performRequest(new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + "_alias" + REQUEST_SEPARATOR + aliasName));
            return true;
        } catch (final IOException ignored) {
        }
        return false;
    }

    @Override
    public void createStoredScript(String scriptName, Map<String, Object> script) throws IOException {

        Request request = new Request(REQUEST_TYPE_POST, REQUEST_SEPARATOR + "_scripts" + REQUEST_SEPARATOR + scriptName);

        performRequest(request, mapWriter.writeValueAsBytes(script));
    }

    @Override
    public ESScriptResponse getStoredScript(String scriptName) throws IOException {

        Request request = new Request(REQUEST_TYPE_GET,
            REQUEST_SEPARATOR + "_scripts" + REQUEST_SEPARATOR + scriptName);

        try {

            final Response response = delegate.performRequest(request);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
            }

            try (final InputStream inputStream = response.getEntity().getContent()) {

                return mapper.readValue(inputStream, new TypeReference<ESScriptResponse>() {
                });

            } catch (final JsonParseException | JsonMappingException | ResponseException e) {
                throw new IOException("Error when we try to parse ES script: " + response.getEntity().getContent());
            }

        } catch (ResponseException e) {

            final Response response = e.getResponse();

            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                ESScriptResponse esScriptResponse = new ESScriptResponse();
                esScriptResponse.setFound(false);
                return esScriptResponse;
            }

            throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
        }
    }

    @Override
    public void createIndex(String indexName, Map<String, Object> settings) throws IOException {

        Request request = new Request(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName);

        Integer clusterHealth = getClusterHealth();
        settings.put("number_of_shards",clusterHealth);
        settings.put("number_of_replicas",(clusterHealth-1));
        if (settings != null && settings.size() > 0) {
            Map<String, Object> updatedSettings = new HashMap<>();
            updatedSettings.put("settings", settings);
            settings = updatedSettings;
        }

        performRequest(request, mapWriter.writeValueAsBytes(settings));
    }

    public Integer getClusterHealth() {
        Request request = new Request("GET", "/_cluster/health");
        try {
            Response response = performRequest(request, null);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final Map<String, Object> health = mapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {
                });
                return Integer.parseInt(health.get("number_of_nodes").toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    @Override
    public void updateIndexSettings(String indexName, Map<String, Object> settings) throws IOException {

        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_settings",
            mapWriter.writeValueAsBytes(settings));
    }

    @Override
    public void updateClusterSettings(Map<String, Object> settings) throws IOException {

        performRequest(REQUEST_TYPE_PUT, REQUEST_SEPARATOR + "_cluster" + REQUEST_SEPARATOR + "settings",
            mapWriter.writeValueAsBytes(settings));
    }

    @Override
    public void addAlias(String alias, String index) throws IOException {
        final Map<String, Object> actionAlias = ImmutableMap.of("actions", ImmutableList.of(ImmutableMap.of("add", ImmutableMap.of("index", index, "alias", alias))));
        performRequest(REQUEST_TYPE_POST, REQUEST_SEPARATOR + "_aliases", mapWriter.writeValueAsBytes(actionAlias));
    }

    @Override
    public Map<String, Object> getIndexSettings(String indexName) throws IOException {
        final Response response = performRequest(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_settings", null);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final Map<String, RestIndexSettings> settings = mapper.readValue(inputStream, new TypeReference<Map<String, RestIndexSettings>>() {
            });
            return settings == null ? null : settings.get(indexName).getSettings().getMap();
        }
    }

    @Override
    public void createMapping(String indexName, String typeName, Map<String, Object> mapping) throws IOException {

        Request request;

//        if(useMappingTypes){
//            request = new Request(REQUEST_TYPE_PUT,
//                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName);
//            if(esVersion7){
//                request.addParameter(INCLUDE_TYPE_NAME_PARAMETER, "true");
//            }
//        } else {
//            request = new Request(REQUEST_TYPE_PUT,
//                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping");
//        }

        request = new Request(REQUEST_TYPE_PUT,
            REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping");
        performRequest(request, mapWriter.writeValueAsBytes(mapping));
    }

    @Override
    public IndexMapping getMapping(String indexName, String typeName) throws IOException {

        Request request;

        if (useMappingTypes) {
            request = new Request(REQUEST_TYPE_GET,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping" + REQUEST_SEPARATOR + typeName);
            if (esVersion7) {
                request.addParameter(INCLUDE_TYPE_NAME_PARAMETER, "true");
            }
        } else {
            request = new Request(REQUEST_TYPE_GET,
                REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_mapping");
        }

        try (final InputStream inputStream = performRequest(request, null).getEntity().getContent()) {

            if (useMappingTypes) {
                final Map<String, TypedIndexMappings> settings = mapper.readValue(inputStream,
                    new TypeReference<Map<String, TypedIndexMappings>>() {
                    });
                return settings != null ? settings.get(indexName).getMappings().get(typeName) : null;
            }

            final Map<String, TypelessIndexMappings> settings = mapper.readValue(inputStream,
                new TypeReference<Map<String, TypelessIndexMappings>>() {
                });
            return settings != null ? settings.get(indexName).getMappings() : null;

        } catch (final JsonParseException | JsonMappingException | ResponseException e) {
            log.info("Error when we try to get ES mapping", e);
            return null;
        }
    }

    @Override
    public void deleteIndex(String indexName) throws IOException {
        if (isAlias(indexName)) {
            // aliased multi-index case
            final String path = REQUEST_SEPARATOR + "_alias" + REQUEST_SEPARATOR + indexName;
            final Response response = performRequest(REQUEST_TYPE_GET, path, null);
            try (final InputStream inputStream = response.getEntity().getContent()) {
                final Map<String, Object> records = mapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {
                });
                if (records == null) return;
                for (final String index : records.keySet()) {
                    if (indexExists(index)) {
                        performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + index, null);
                    }
                }
            }
        }
    }

    @Override
    public void clearStore(String indexName, String storeName) throws IOException {
        String name = indexName + "_" + storeName;
        if (indexExists(name)) {
            performRequest(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + indexName + "_" + storeName, null);
        }
    }


    @Override
    public void bulkRequest(List<OpenSearchMutation> requests, String ingestPipeline) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (final OpenSearchMutation request : requests) {
            Map<String, Object> requestData = new HashMap<>();

//            if (useMappingTypes) {
//                requestData.put("_index", request.getIndex());
//                requestData.put("_type", request.getType());
//                requestData.put("_id", request.getId());
//            } else {
//                requestData.put("_index", request.getIndex());
//                requestData.put("_id", request.getId());
//            }

            requestData.put("_index", request.getIndex());
            requestData.put("_id", request.getId());

            if (retryOnConflict != null && request.getRequestType() == OpenSearchMutation.RequestType.UPDATE) {
                requestData.put(retryOnConflictKey, retryOnConflict);
            }

            outputStream.write(mapWriter.writeValueAsBytes(
                ImmutableMap.of(request.getRequestType().name().toLowerCase(), requestData))
            );
            outputStream.write(NEW_LINE_BYTES);
            if (request.getSource() != null) {
                outputStream.write(mapWriter.writeValueAsBytes(request.getSource()));
                outputStream.write(NEW_LINE_BYTES);
            }
        }

        final StringBuilder builder = new StringBuilder();
        if (ingestPipeline != null) {
            APPEND_OP.apply(builder).append("pipeline=").append(ingestPipeline);
        }
        if (bulkRefreshEnabled) {
            APPEND_OP.apply(builder).append("refresh=").append(bulkRefresh);
        }
        builder.insert(0, REQUEST_SEPARATOR + "_bulk");

        final Response response = performRequest(REQUEST_TYPE_POST, builder.toString(), outputStream.toByteArray());
        try (final InputStream inputStream = response.getEntity().getContent()) {
            final RestBulkResponse bulkResponse = mapper.readValue(inputStream, RestBulkResponse.class);
            final List<Object> errors = bulkResponse.getItems().stream()
                .flatMap(item -> item.values().stream())
                .filter(item -> item.getError() != null && item.getStatus() != 404)
                .map(RestBulkResponse.RestBulkItemResponse::getError).collect(Collectors.toList());
            if (!errors.isEmpty()) {
                errors.forEach(error -> log.error("Failed to execute ES query: {}", error));
                throw new IOException("Failure(s) in Opensearch bulk request: " + errors);
            }
        }
    }

    public void setRetryOnConflict(Integer retryOnConflict) {
        this.retryOnConflict = retryOnConflict;
    }

    @Override
    public long countTotal(String indexName, Map<String, Object> requestData) throws IOException {

        final Request request = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_count");

        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Opensearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestCountResponse.class).getCount();
        }
    }

    /**
     * Execute the aggregation request using Elasticsearch index.
     * Elasticsearch uses double values to hold and represent numeric data. As a result, aggregations on long numbers
     * greater than 2^53 are approximate.
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/7.17/search-aggregations.html#limits-for-long-values">Elasticsearch, limits for long values</a>
     *
     * @param indexName   the name of the ElasticSearch index on which the aggregation is executed
     * @param requestData the filter query
     * @param agg         the name of the aggregation operation (min, max, avg, sum)
     * @param fieldName   the name of the field on which the aggregation is computed
     * @return the result of the aggregation
     * @throws IOException 异常
     */
    private double executeAggs(String indexName, Map<String, Object> requestData, String agg, String fieldName) throws IOException {

        final Request request = new Request(REQUEST_TYPE_GET, REQUEST_SEPARATOR + indexName + REQUEST_SEPARATOR + "_search");

        requestData.put("aggs", ImmutableMap.of("agg_result", ImmutableMap.of(agg, ImmutableMap.of("field", fieldName))));
        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Elasticsearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestAggResponse.class).getAggregations().getAggResult().getValue();
        }
    }

    private Number adaptNumberType(double value, Class<? extends Number> expectedType) {
        if (expectedType == null) return value;
        else if (Byte.class.isAssignableFrom(expectedType)) return (byte) value;
        else if (Short.class.isAssignableFrom(expectedType)) return (short) value;
        else if (Integer.class.isAssignableFrom(expectedType)) return (int) value;
        else if (Long.class.isAssignableFrom(expectedType)) return (long) value;
        else if (Float.class.isAssignableFrom(expectedType)) return (float) value;
        else return value;
    }

    @Override
    public Number min(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        return adaptNumberType(executeAggs(indexName, requestData, "min", fieldName), expectedType);
    }

    @Override
    public Number max(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        return adaptNumberType(executeAggs(indexName, requestData, "max", fieldName), expectedType);
    }

    @Override
    public double avg(String indexName, Map<String, Object> requestData, String fieldName) throws IOException {
        return executeAggs(indexName, requestData, "avg", fieldName);
    }

    @Override
    public Number sum(String indexName, Map<String, Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException {
        Class<? extends Number> returnType;
        double sum = executeAggs(indexName, requestData, "sum", fieldName);
        if (Float.class.isAssignableFrom(expectedType) || Double.class.isAssignableFrom(expectedType))
            return sum;
        else
            return (long) sum;
    }

    @Override
    public RestSearchResponse search(String indexName, Map<String, Object> requestData, boolean useScroll) throws IOException {
        final StringBuilder path = new StringBuilder(REQUEST_SEPARATOR).append(indexName);
        path.append(REQUEST_SEPARATOR).append("_search");
        if (useScroll) {
            path.append(REQUEST_PARAM_BEGINNING).append("scroll=").append(scrollKeepAlive);
        }
        return search(requestData, path.toString());
    }

    @Override
    public RestSearchResponse search(String scrollId) throws IOException {
        final Map<String, Object> requestData = new HashMap<>();
        requestData.put("scroll", scrollKeepAlive);
        requestData.put("scroll_id", scrollId);
        return search(requestData, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll");
    }

    @Override
    public void deleteScroll(String scrollId) throws IOException {
        delegate.performRequest(new Request(REQUEST_TYPE_DELETE, REQUEST_SEPARATOR + "_search" + REQUEST_SEPARATOR + "scroll" + REQUEST_SEPARATOR + scrollId));
    }

    public void setBulkRefresh(String bulkRefresh) {
        this.bulkRefresh = bulkRefresh;
        bulkRefreshEnabled = bulkRefresh != null && !bulkRefresh.equalsIgnoreCase("false");
    }

    private RestSearchResponse search(Map<String, Object> requestData, String path) throws IOException {

        final Request request = new Request(REQUEST_TYPE_POST, path);

        final byte[] requestDataBytes = mapper.writeValueAsBytes(requestData);
        if (log.isDebugEnabled()) {
            log.debug("Opensearch request: " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestData));
        }

        final Response response = performRequest(request, requestDataBytes);
        try (final InputStream inputStream = response.getEntity().getContent()) {
            return mapper.readValue(inputStream, RestSearchResponse.class);
        }
    }

    private Response performRequest(String method, String path, byte[] requestData) throws IOException {
        return performRequest(new Request(method, path), requestData);
    }

    private Response performRequest(Request request, byte[] requestData) throws IOException {

        final HttpEntity entity = requestData != null ? new ByteArrayEntity(requestData, ContentType.APPLICATION_JSON) : null;

        request.setEntity(entity);

        final Response response = delegate.performRequest(request);

        if (response.getStatusLine().getStatusCode() >= 400) {
            throw new IOException("Error executing request: " + response.getStatusLine().getReasonPhrase());
        }
        return response;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class ClusterInfo {

        private Map<String, Object> version;

        public Map<String, Object> getVersion() {
            return version;
        }

        public void setVersion(Map<String, Object> version) {
            this.version = version;
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static final class ClusterHealthInfo {

        public Map<String, Object> getNumber_of_nodes() {
            return number_of_nodes;
        }

        public void setNumber_of_nodes(Map<String, Object> number_of_nodes) {
            this.number_of_nodes = number_of_nodes;
        }

        private Map<String, Object> number_of_nodes;

    }
}
