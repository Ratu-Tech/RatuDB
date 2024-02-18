package org.apache.ratu.second.esclient;


import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.Version;
import org.opensearch.client.*;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.env.Environment.PATH_HOME_SETTING;


public class ElasticIndex {

    private static final Logger logger = LoggerFactory.getLogger(ElasticIndex.class);

    private final List<String> partitionKeysNames;
    private final List<String> clusteringColumnsNames;
    private final boolean hasClusteringColumns;

    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY =
        new NamedXContentRegistry(ClusterModule.getNamedXWriteables());

    private static final String ES_SOURCE = "_source";

    private static final Settings SETTINGS = Settings.builder()
        .put(PATH_HOME_SETTING.getKey(), "dummy")
        .build();


    private static List<HttpHost> clusterHosts;

    private static RestClient client;

    public static final String TRUSTSTORE_PATH = "truststore.path";
    public static final String TRUSTSTORE_PASSWORD = "truststore.password";
    public static final String CLIENT_SOCKET_TIMEOUT = "client.socket.timeout";
    public static final String CLIENT_PATH_PREFIX = "client.path.prefix";

    private static TreeSet<Version> nodeVersions;


    public ElasticIndex(@Nonnull List<String> partitionKeysNames, @Nonnull List<String> clusteringColumnsNames) {
        this.partitionKeysNames = partitionKeysNames;
        this.clusteringColumnsNames = clusteringColumnsNames;
        this.hasClusteringColumns = !clusteringColumnsNames.isEmpty();
        List<String> restCluster = getTestRestCluster();

        List<HttpHost> hosts = new ArrayList<>(restCluster.size());
        for (int i = 0; i < restCluster.size(); i++) {
            String[] split = restCluster.get(i).split(":");
            hosts.add(new HttpHost(HttpHost.DEFAULT_SCHEME.name(), split[0], 9200));
        }
        clusterHosts = unmodifiableList(hosts);
        nodeVersions = new TreeSet<>();
        logger.debug("initializing REST clients against {}", clusterHosts);
        try {
            if (client == null) {
                synchronized (ElasticIndex.class) {
                    if (client == null) {
                        client = buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[clusterHosts.size()]));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Boolean newIndex(String indexName, Settings setting, Map<String, Map<String, String>> fields) {
        Boolean result = false;
        try {
            result = createIndex(indexName, setting, parseEsCreateIndexMappings(fields));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Index " + indexName + " Create Exception:", e);
        }
        return result;
    }

    public Boolean newRollOverIndex(String indexName, Settings setting, Map<String, Map<String, String>> fields,Integer minDocCount) throws Exception {
        Boolean result = createRollOverIndex(indexName, setting, parseEsCreateIndexMappings(fields),minDocCount);
        return result;
    }


    public Boolean isExistsIndex(String indexName) {
        // 判断索引是否存在
        try {
            return indexExists(indexName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void indexData(Map<String, Object> maps, String indexName, String primaryKeyValue, Boolean asyncWrite,Boolean isRollOver) {
        // 索引数据
        try {
            bulkIndexData(maps,indexName,primaryKeyValue, asyncWrite,isRollOver);
        } catch (Exception e) {
            logger.error("Add Index Data to " + indexName + " Exception:", e);
        }
    }




    public Boolean delData(String indexName, String primaryKeyValue,Boolean isRollOver) throws IOException {
        // 删除数据
        return delIndexData(indexName, primaryKeyValue,isRollOver);
    }

    public void dropIndex(String indexName) {
        // 删除索引
        try {
            delIndexByName(indexName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public boolean refreshData(String indexName) {
        // refresh 索引
        try {
            return refreshIndex(indexName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public SearchResult searchData(String indexName, Map<String, Object> mappings) {
        // 普通搜索
        List<SearchResultRow> rowList = new ArrayList<>();
        Map<String, Object> query = (Map<String, Object>) mappings.get("query");
        query.put("size",mappings.get("size"));
        query.put("from",mappings.get("from"));

        String json = "";
        if (query != null) {
            if (query.get("bool") != null) {
                json = parseEsBool(query);
            } else {
                json = parseEsQuery(query);
            }
        }
        try {
            Map<String, Object> mp = searchIndexData(indexName, json);

            List<String> primaryKeys;

            if (hasClusteringColumns) {
                primaryKeys = new ArrayList<>(partitionKeysNames.size() + clusteringColumnsNames.size());
                primaryKeys.addAll(partitionKeysNames);
                primaryKeys.addAll(clusteringColumnsNames);
            } else {
                primaryKeys = partitionKeysNames;
            }

            int pkSize = primaryKeys.size();

            Map<String, Object> hitsMap = (Map<String, Object>) mp.get("hits");

            List<Map<String, Object>> hits2List = (List<Map<String, Object>>) hitsMap.get("hits");


            for (int i = 0; i < hits2List.size(); i++) {

                Map<String, Object> sourceMaps = (Map<String, Object>) hits2List.get(i).get("_source");
                String[] primaryKey = new String[pkSize];
                int keyNb = 0;

                for (String keyName : primaryKeys) {
                    String value = sourceMaps.get(keyName).toString();
                    if (value == null) {
                        continue;
                    } else {
                        primaryKey[keyNb] = value;
                    }
                    keyNb++;
                }

                SearchResultRow searchResultRow = new SearchResultRow(primaryKey, new JSONObject(sourceMaps));
                rowList.add(searchResultRow);
            }
//            Map<String, Aggregate> aggregations = search.aggregations();
//            if (aggregations.size() != 0) {
//                Map<Object, Object> map = AggsUtils.aggsData(aggregations.get("aggs"));
//                String[] primaryKey = new String[1];
//                primaryKey[0] = String.valueOf(search.hits().hits().size() + 1);
//                SearchResultRow row = new SearchResultRow(primaryKey, JSONObject.parseObject(JSON.toJSONString(map)));
//                rowList.add(row);
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new SearchResult(rowList);
    }

    private String parseAggs(Map<String, Object> mappings) {
        return mappings.get("aggs").toString();
    }


    // 普通查询
    private static String parseEsQuery(Map<String, Object> queryMps) {
        Map<String, Object> aggsMaps = new HashMap<>();
        Map<String, Object> fieldMaps = new HashMap<>();
        Map<String, Object> whereMaps = new HashMap<>();
        for (String m : queryMps.keySet()) {
            if (!m.equals("type") && !m.equals("field")&& !m.equals("size")&&!m.equals("from")) {
                whereMaps.put(m, queryMps.get(m));
            }
        }
        Object value = queryMps.get("value");
        if (value == null) {
            fieldMaps.put(queryMps.get("field").toString(), whereMaps);
        } else {
            fieldMaps.put(queryMps.get("field").toString(), value);
        }
        aggsMaps.put(queryMps.get("type").toString(), fieldMaps);
        Map<String, Object> query = new HashMap<>();
        query.put("query", aggsMaps);
        query.put("size", queryMps.get("size"));
        query.put("from", queryMps.get("from"));
        return JSON.toJSONString(query);
    }

    // bool 查询
    private static String parseEsBool(Map<String, Object> boolMps) {
        Map<String, Object> aggsMaps = new HashMap<>();
        Map<String, List<Map<String, Object>>> filter = (Map<String, List<Map<String, Object>>>) boolMps.get("bool");
        Map<String, Object> boolWhere = new HashMap<>();
        for (String key : filter.keySet()) {
            List<Object> list = new ArrayList<>();
            List<Map<String, Object>> mapList = filter.get(key);
            mapList.stream().forEach(mps -> {
                String s = parseEsQuery(mps);
                Map map = JSONObject.parseObject(s, Map.class);
                list.add(map);
            });
            boolWhere.put(key, list);
        }
        aggsMaps.put("bool", boolWhere);
        Map<String, Object> query = new HashMap<>();
        query.put("query", aggsMaps);
        //query.put("size", boolMps.get("size"));
        //query.put("from", boolMps.get("from"));
        return JSON.toJSONString(query);
    }

    //创建索引格式mappings
    private static String parseEsCreateIndexMappings(Map<String, Map<String, String>> fields) {
        HashMap<String, Object> map1 = new HashMap<>();
        for (String key : fields.keySet()) {
            HashMap<String, Object> map2 = new HashMap<>();
            Map<String, String> filedmap = fields.get(key);
            for (String key2 : filedmap.keySet()) {
                String value2 = filedmap.get(key2);
                if (key2.equals("type") || key2.equals("analyzer")) {
                    map2.put(key2, value2);
                    if (value2.equals("text")) {
                        Map<String, Object> childMap = new HashMap<>();
                        childMap.put("type", "keyword");
                        childMap.put("ignore_above", 256);
                        Map<String, Object> keywordMap = new HashMap<>();
                        keywordMap.put("keyword", childMap);
                        map2.put("fields", keywordMap);
                    }

                }
            }
            map1.put(key, map2);
        }

        HashMap<String, Object> mappings = new HashMap<>();

        mappings.put("properties", map1);
        return JSON.toJSONString(mappings);
    }

    protected List<String> getTestRestCluster() {
        List<String> list = new ArrayList<>();
        DatabaseDescriptor.getSeeds().stream().forEach(seeds -> {
            String hostAddressAndPort = seeds.getHostAddressAndPort();
            list.add(hostAddressAndPort);
        });
        return list;
    }

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        return RestClient.builder(hosts).setHttpClientConfigCallback((HttpAsyncClientBuilder httpAsyncClientBuilder) -> httpAsyncClientBuilder).build();
    }

    protected static void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        String keystorePath = settings.get(TRUSTSTORE_PATH);
        if (keystorePath != null) {
            final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
            if (keystorePass == null) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
            }
            Path path = PathUtils.get(keystorePath);
            if (!Files.exists(path)) {
                throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
            }
            try {
                final String keyStoreType = keystorePath.endsWith(".p12") ? "PKCS12" : "jks";
                KeyStore keyStore = KeyStore.getInstance(keyStoreType);
                try (InputStream is = Files.newInputStream(path)) {
                    keyStore.load(is, keystorePass.toCharArray());
                }
                final SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
                builder.setHttpClientConfigCallback(httpClientBuilder -> {
                    final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setSslContext(sslcontext)
                        // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                        .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                            @Override
                            public TlsDetails create(final SSLEngine sslEngine) {
                                return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                            }
                        })
                        .build();

                    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                        .setTlsStrategy(tlsStrategy)
                        .build();

                    return httpClientBuilder.setConnectionManager(connectionManager);
                });
            } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
                throw new RuntimeException("Error setting up ssl", e);
            }
        }
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(
            conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis())))
        );
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put("xpack.security.user", "elastic-admin:elastic-password");
        return builder.build();
    }

    protected static RestClient client() {
        return client;
    }

    protected static Boolean createIndex(String name, Settings settings) throws IOException {
        return createIndex(name, settings, null);
    }

    protected static Boolean createIndex(String name, Settings settings, String mapping) throws IOException {
        return createIndex(name, settings, mapping, null);
    }

    protected static Boolean createRollOverIndex(String name, Settings settings, String mapping,Integer minDocCount) throws IOException {
        return createRollOverIndex(name, settings, mapping, null,minDocCount);
    }

    protected static Boolean createIndex(String name, Settings settings, String mapping, String aliases) throws IOException {
        Request request = new Request("PUT", "/" + name);
        String entity = "{\"settings\": " + Strings.toString(XContentType.JSON, settings);
        if (mapping != null) {
            entity += ",\"mappings\" : " + mapping;
        }
        if (aliases != null) {
            entity += ",\"aliases\": {" + aliases + "}";
        }
        entity += "}";
        if (settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        } else if (settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()) ||
            settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey())) {
            expectTranslogRetentionWarning(request);
        }
        request.setJsonEntity(entity);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        return Boolean.parseBoolean(map.get("acknowledged").toString());
    }

    protected static Boolean createRollOverIndex(String name, Settings settings, String mapping, String aliases,Integer minDocCount) throws IOException {
        createRollOverPolicies(name,minDocCount);
        createRolloverAlias(name);

        String indexName = name+"-000001";
        Request request = new Request("PUT", "/" + indexName);
        String entity = "{\"settings\": " + Strings.toString(XContentType.JSON, settings);
        if (mapping != null) {
            entity += ",\"mappings\" : " + mapping;
        }
        if (StringUtils.isBlank(aliases)) {
            String createReName = "{\n" +
                "    \"" + name + "\": {\n" +
                "      \"is_write_index\": true\n" +
                "    }\n" +
                "  }";
            entity += ",\"aliases\":"+ createReName;
        }else{
            String createReName = "{\n" +
                "    \"" + name + "\": {\n" +
                "      \"is_write_index\": true\n" +
                "    }\n" +
                "  }";

            entity += ",\"aliases\": {"
                + aliases
                + ","
                + createReName
                + "}";
        }
        entity += "}";
        if (settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        } else if (settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()) ||
            settings.hasValue(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey())) {
            expectTranslogRetentionWarning(request);
        }
        request.setJsonEntity(entity);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        return Boolean.parseBoolean(map.get("acknowledged").toString());
    }

    private static Boolean createRolloverAlias(String index)throws IOException{
        Request request = new Request("PUT", "_index_template/"+index);

        String entity = "{\n" +
            "  \"index_patterns\": [\"" + index + "*\"],\n" +
            "  \"template\": {\n" +
            "   \"settings\": {\n" +
            "    \"plugins.index_state_management.rollover_alias\": \"" + index + "\"\n" +
            "   }\n" +
            " }\n" +
            "}";

        request.setJsonEntity(entity);
        Response response = client().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            return false;
        }
        return true;
    }

    private static Boolean createRollOverPolicies(String index,Integer minDocCount) throws IOException{
        Request request = new Request("PUT", "_plugins/_ism/policies/"+index);

        String entity = "{\n" +
            "   \"policy\": {\n" +
            "     \"description\": \"this is description!!!\",\n" +
            "     \"default_state\": \"rollover\",\n" +
            "     \"states\": [\n" +
            "       {\n" +
            "         \"name\": \"rollover\",\n" +
            "         \"actions\": [\n" +
            "           {\n" +
            "             \"rollover\": {\n" +
            "                 \"min_size\":\"10gb\","+
            "                 \"min_doc_count\":"+minDocCount+"\n"+
            //"               \"min_size\":\"" + rollover +"\""+
            //"               \"min_doc_count\":\"" + rollover +"\""+
            "             }\n" +
            "           }\n" +
            "         ],\n" +
            "         \"transitions\": []\n" +
            "       }\n" +
            "     ],\n" +
            "     \"ism_template\": {\n" +
            "       \"index_patterns\": [\"" + index + "*\"],\n" +
            "       \"priority\": 100\n" +
            "     }\n" +
            "   }\n" +
            " }";
        request.setJsonEntity(entity);
        Response response = client().performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            return false;
        }
        return true;
    }


    private static Boolean isExistIndexTemplates(String index) throws IOException {
        Request request = new Request("GET", "/_index_template/" + index);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        return ObjectUtils.isEmpty(map.get("error"));
    }


    private static Boolean bulkIndexData(Map<String, Object> maps,String indexName,String primaryKeyValue, Boolean asyncWrite,Boolean isRollOver) throws IOException {
        String json = "{ \"index\" : { \"_index\" : \"" +
            indexName + "\", \"_id\" : \"" +
            primaryKeyValue + "\" } }\n" +
            JSON.toJSONString(maps) + "\n";

        //?filter_path=errors
        Request request = new Request("POST", "_bulk");
        request.setJsonEntity(json);
        if (!asyncWrite) {
            Response response = client().performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                return false;
            }
            return true;
        } else {
            client().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {

                }

                @Override
                public void onFailure(Exception exception) {
                    logger.error("Async Write Data to " + indexName + " Execption", exception);
                }
            });
        }
        return true;
    }

    private static Boolean addIndexData(String index, String json, String id, Boolean asyncWrite) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.setJsonEntity(json);
        request.addParameter("timeout", "1000ms");
        if (!asyncWrite) {
            Response response = client().performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                return false;
            }
            return true;

        } else {
            client().performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    //logger.debug("Async Write Data Response:"+response);
                }

                @Override
                public void onFailure(Exception exception) {
                    logger.error("Async Write Data to " + index + " Execption", exception);
                }
            });
            return true;
        }
    }

    private static String getIndexById(String index, String id) throws IOException {
        Request request = new Request("POST", "/" + index + "/_search/");
        request.setJsonEntity(" {\n" +
            "    \"query\": {\n" +
            "        \"ids\": {\n" +
            "            \"values\": [\n" +
            "                "+ id +"\n" +
            "            ]\n" +
            "        }\n" +
            "    }\n" +
            "}");

        try {
            Response response = client().performRequest(request);

            Map<String, Object> map = entityAsMap(response);
            Map<String, Object> hitsMap = (Map<String, Object>) map.get("hits");

            List<Map<String, Object>> hits2List = (List<Map<String, Object>>) hitsMap.get("hits");
            if (hits2List.isEmpty()) {
                return index;
            }
            return hits2List.get(0).get("_index").toString();
        }catch (Exception e){
            return index;
        }
    }

    private static Boolean delIndexData(String index, String id,Boolean isRollOver) throws IOException {
        String indexName = isRollOver ? getIndexById(index,id) : index;

        //兼容滚动索引的数据删除
        Request request = new Request("DELETE", "/" + indexName + "/_doc/" + id);

        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        String result = map.get("result").toString();
        if (result.equals("deleted")) {
            return true;
        }
        return false;
    }

    /**
     * 删除索引分两类
     * 1.普通索引
     *    删除时可直接删除
     * 2.滚动索引
     *    由于创建滚动索引时定义了索引模版和规则，所以需要将对应规则也删掉
     *    1）.删除对应规则
     *    2）.删除对应索引（由于索引可能已经滚动可能是多个）
     * */
    private static Boolean delIndexByName(String index) throws IOException {
        getIndexJoin(index).forEach(var ->{
            Request request = new Request("DELETE", "/" + var);
            try {
                Response response = client().performRequest(request);
                entityAsMap(response);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        delIndexPolicies(index);
        return delIndexTemplates(index);
    }

    private static Boolean delIndexPolicies(String index)throws IOException {
        Request request = new Request("DELETE", "/_plugins/_ism/policies/" + index);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        String result = map.get("result").toString();
        if (result.equals("deleted")) {
            return true;
        }
        return false;
    }

    private static Boolean delIndexTemplates(String index) throws IOException {
        Request request = new Request("DELETE", "/_index_template/" + index);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        String result = map.get("acknowledged").toString();
        return Boolean.valueOf(result);
    }
    /**
     * 根据索引别名获取关联索引
     */
    private static Set<String> getIndexJoin(String index) throws IOException {
        Request request = new Request("GET", "/" + index);
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        return map.keySet();
    }

    private static Boolean refreshIndex(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_refresh");
        Response response = client().performRequest(request);
        Map<String, Object> map = entityAsMap(response);
        Map shardsMaps = (Map) map.get("_shards");
        Object successful = shardsMaps.get("successful");
        return Boolean.valueOf(successful.toString());
    }


    protected static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    public Integer getClusterHealth() {
        Request request = new Request("GET", "/_cluster/health");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> map = entityAsMap(response);
            return Integer.valueOf(map.get("number_of_nodes").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    private static Map<String, Object> searchIndexData(String index, String dslJson) throws IOException {
        Request request = new Request("GET", "/" + index + "/_search");
        if (!StringUtils.isBlank(dslJson)) {
            request.setJsonEntity(dslJson);
        }
        logger.info("dslJson------------>"+dslJson);
        logger.info("dslJson------------>"+dslJson);
        logger.info("dslJson------------>"+dslJson);
        logger.info("dslJson------------>"+dslJson);
        logger.info("dslJson------------>"+dslJson);
        Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    protected static void expectSoftDeletesWarning(Request request, String indexName) {
        final List<String> expectedWarnings = Collections.singletonList(
            "Creating indices with soft-deletes disabled is deprecated and will be removed in future Elasticsearch versions. " +
                "Please do not specify value for setting [index.soft_deletes.enabled] of index [" + indexName + "].");
        final RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_2_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        } else if (nodeVersions.stream().anyMatch(version -> version.onOrAfter(Version.V_2_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.isEmpty() == false && warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        }
    }


    protected static void expectTranslogRetentionWarning(Request request) {
        final List<String> expectedWarnings = Collections.singletonList(
            "Translog retention settings [index.translog.retention.age] "
                + "and [index.translog.retention.size] are deprecated and effectively ignored. They will be removed in a future version.");
        final RequestOptions.Builder requestOptions = RequestOptions.DEFAULT.toBuilder();
        if (nodeVersions.stream().allMatch(version -> version.onOrAfter(Version.V_2_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        } else if (nodeVersions.stream().anyMatch(version -> version.onOrAfter(Version.V_2_6_0))) {
            requestOptions.setWarningsHandler(warnings -> warnings.isEmpty() == false && warnings.equals(expectedWarnings) == false);
            request.setOptions(requestOptions);
        }
    }


    /**
     * Convert the entity from a {@link Response} into a map of maps.
     */
    public static Map<String, Object> entityAsMap(Response response) throws IOException {
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType());
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            return parser.map();
        }
    }
}
