package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.opensearch.mapping.IndexMapping;
import org.janusgraph.diskstorage.opensearch.script.ESScriptResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface OpenSearchClient extends Closeable {

    OpenMajorVersion getMajorVersion();

    void clusterHealthRequest(String timeout) throws IOException;

    boolean indexExists(String indexName) throws IOException;

    boolean isIndex(String indexName);

    boolean isAlias(String aliasName);

    void createStoredScript(String scriptName, Map<String,Object> script) throws IOException;

    ESScriptResponse getStoredScript(String scriptName) throws IOException;

    void createIndex(String indexName, Map<String,Object> settings) throws IOException;

    void updateIndexSettings(String indexName, Map<String,Object> settings) throws IOException;

    void updateClusterSettings(Map<String,Object> settings) throws IOException;

    Map<String,Object> getIndexSettings(String indexName) throws IOException;

    void createMapping(String indexName, String typeName, Map<String,Object> mapping) throws IOException;

    IndexMapping getMapping(String indexName, String typeName) throws IOException;

    void deleteIndex(String indexName) throws IOException;

    void clearStore(String indexName, String storeName) throws IOException;

    void bulkRequest(List<OpenSearchMutation> requests, String ingestPipeline) throws IOException;

    long countTotal(String indexName, Map<String,Object> requestData) throws IOException;

    Number min(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    Number max(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    double avg(String indexName, Map<String,Object> requestData, String fieldName) throws IOException;

    Number sum(String indexName, Map<String,Object> requestData, String fieldName, Class<? extends Number> expectedType) throws IOException;

    OpenSearchResponse search(String indexName, Map<String,Object> request, boolean useScroll) throws IOException;

    OpenSearchResponse search(String scrollId) throws IOException;

    void deleteScroll(String scrollId) throws IOException;

    void addAlias(String alias, String index) throws IOException;

}
