/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratu;

import com.alibaba.fastjson2.JSONObject;
import com.jcabi.manifests.Manifests;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Scriptengines;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.janusgraph.graphdb.grpc.JanusGraphContextHandler;
import org.janusgraph.graphdb.grpc.JanusGraphManagerServiceImpl;
import org.janusgraph.graphdb.grpc.schema.SchemaManagerImpl;
import org.janusgraph.graphdb.management.JanusGraphManager;
import org.janusgraph.graphdb.server.JanusGraphSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class JanusgraphServer {

    private static final Logger logger = LoggerFactory.getLogger(JanusgraphServer.class);


    private GremlinServer gremlinServer = null;
    private JanusGraphSettings janusGraphSettings = null;
    private final String confPath;
    private CompletableFuture<Void> serverStarted = null;
    private CompletableFuture<Void> serverStopped = null;
    private Server grpcServer = null;
    public static final String MANIFEST_JANUSGRAPH_VERSION_ATTRIBUTE = "janusgraphVersion";
    public static final String MANIFEST_TINKERPOP_VERSION_ATTRIBUTE = "tinkerpopVersion";

    private final String cassandraConfigFile;

    public JanusgraphServer(String file, String _configFile) {
        this.confPath = file;
        this.cassandraConfigFile = _configFile;
    }

    public static void active() {

        try {
            String janusgraphConfig = System.getProperty("opensearch.path.conf");
            String janusgraphHome = System.getProperty("opensearch.path.home");
            String file = janusgraphConfig + "/gremlin-server-cql-opensearch.yaml";
            System.setProperty("log4j2.configurationFile", "file:" + janusgraphConfig + "/log4j2.xml");
            System.setProperty("javaagent", janusgraphHome + "/lib/jamm-0.3.2.jar");
            String cassandraYaml = janusgraphConfig + "/cassandra.yaml";
            printHeader();

            JanusgraphServer janusGraphServer = new JanusgraphServer(file, cassandraYaml);
            janusGraphServer.start().exceptionally(t -> {
                logger.error("JanusGraph Server was unable to start and will now begin shutdown", t);
                janusGraphServer.stop().join();
                return null;
            }).join();

        } catch (Exception e) {
            logger.error("JanusgraphServer 启动错误:", e);
        }
    }


    private Server createGrpcServer(JanusGraphSettings janusGraphSettings, GraphManager graphManager) {
        JanusGraphContextHandler janusGraphContextHandler = new JanusGraphContextHandler(graphManager);
        return ServerBuilder.forPort(janusGraphSettings.getGrpcServer().getPort()).addService(new JanusGraphManagerServiceImpl(janusGraphContextHandler)).addService(new SchemaManagerImpl(janusGraphContextHandler)).build();
    }

    public synchronized CompletableFuture<Void> start() {
        if (this.serverStarted != null) {
            return this.serverStarted;
        } else {
            this.serverStarted = new CompletableFuture();

            try {
                logger.info("Configuring JanusGraph Server from {}", this.confPath);


                initializationSettings();

                //this.janusGraphSettings = JanusGraphSettings.read(this.confPath);

                this.gremlinServer = new GremlinServer(this.janusGraphSettings);
                CompletableFuture<Void> grpcServerFuture = CompletableFuture.completedFuture((Void) null);
                if (this.janusGraphSettings.getGrpcServer().isEnabled()) {
                    grpcServerFuture = CompletableFuture.runAsync(() -> {
                        GraphManager graphManager = this.gremlinServer.getServerGremlinExecutor().getGraphManager();
                        this.grpcServer = this.createGrpcServer(this.janusGraphSettings, graphManager);

                        try {
                            this.grpcServer.start();
                        } catch (IOException var3) {
                            throw new IllegalStateException(var3);
                        }
                    });
                }

                CompletableFuture<Void> gremlinServerFuture = this.gremlinServer.start().thenAcceptAsync(JanusgraphServer::configure);
                this.serverStarted = CompletableFuture.allOf(gremlinServerFuture, grpcServerFuture);
            } catch (Exception var3) {
                this.serverStarted.completeExceptionally(var3);
            }

            return this.serverStarted;
        }
    }


    private void initializationSettings() {
        this.janusGraphSettings = new JanusGraphSettings();
        this.janusGraphSettings.authentication.authenticator = "org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator";
        this.janusGraphSettings.channelizer = DatabaseDescriptor.getJanusgraph_channelizer();
        this.janusGraphSettings.enableAuditLog = false;
        this.janusGraphSettings.evaluationTimeout = DatabaseDescriptor.getJanusgraph_evaluationtimeout();
        this.janusGraphSettings.graphManager = DatabaseDescriptor.getJanusgraph_graphmanager();
        this.janusGraphSettings.gremlinPool = 0;
        JanusGraphSettings.GrpcServerSettings grpc = new JanusGraphSettings.GrpcServerSettings();
        grpc.setEnabled(false);
        grpc.setPort(10182);
        this.janusGraphSettings.setGrpcServer(grpc);

        this.janusGraphSettings.host = DatabaseDescriptor.getRpcAddress().getHostAddress();
        this.janusGraphSettings.idleConnectionTimeout = 0;
        this.janusGraphSettings.keepAliveInterval = 0;
        this.janusGraphSettings.maxAccumulationBufferComponents = DatabaseDescriptor.getJanusgraph_maxaccumulationbuffercomponents();
        this.janusGraphSettings.maxChunkSize = DatabaseDescriptor.getJanusgraph_maxchunksize();
        this.janusGraphSettings.maxContentLength = DatabaseDescriptor.getJanusgraph_maxcontentlength();
        this.janusGraphSettings.maxHeaderSize = DatabaseDescriptor.getJanusgraph_maxheadersize();
        this.janusGraphSettings.maxInitialLineLength = DatabaseDescriptor.getJanusgraph_maxinitiallinelength();
        this.janusGraphSettings.maxParameters = 16;
        this.janusGraphSettings.maxSessionTaskQueueSize = 4096;
        this.janusGraphSettings.maxWorkQueueSize = 8192;
        this.janusGraphSettings.metrics = new Settings.ServerMetrics();
        this.janusGraphSettings.metrics.consoleReporter = new Settings.ConsoleReporterMetrics();
        this.janusGraphSettings.metrics.consoleReporter.enabled = true;
        this.janusGraphSettings.metrics.consoleReporter.interval = 180000;
        this.janusGraphSettings.metrics.csvReporter = new Settings.CsvReporterMetrics();
        this.janusGraphSettings.metrics.csvReporter.enabled = true;
        this.janusGraphSettings.metrics.csvReporter.fileName = "/tmp/gremlin-server-metrics.csv";
        this.janusGraphSettings.metrics.csvReporter.interval = 180000;

        this.janusGraphSettings.metrics.graphiteReporter = new Settings.GraphiteReporterMetrics();
        this.janusGraphSettings.metrics.graphiteReporter.enabled = false;
        this.janusGraphSettings.metrics.graphiteReporter.host = "localhost";
        this.janusGraphSettings.metrics.graphiteReporter.interval = 180000;
        this.janusGraphSettings.metrics.graphiteReporter.port = 2003;
        this.janusGraphSettings.metrics.graphiteReporter.prefix = "";

        this.janusGraphSettings.metrics.jmxReporter = new Settings.JmxReporterMetrics();
        this.janusGraphSettings.metrics.jmxReporter.enabled = true;

        this.janusGraphSettings.metrics.slf4jReporter = new Settings.Slf4jReporterMetrics();
        this.janusGraphSettings.metrics.slf4jReporter.enabled = true;
        this.janusGraphSettings.metrics.slf4jReporter.interval = 180000;
        this.janusGraphSettings.metrics.slf4jReporter.loggerName = "org.apache.tinkerpop.gremlin.server.Settings$Slf4jReporterMetrics";

        this.janusGraphSettings.port = DatabaseDescriptor.getJanusgraph_port();

        Settings.ProcessorSettings settings1 = new Settings.ProcessorSettings();
        settings1.className = "org.apache.tinkerpop.gremlin.server.op.session.SessionOpProcessor";
        settings1.config = new HashMap<>();
        settings1.config.put("sessionTimeout", 28800000);
        Settings.ProcessorSettings settings2 = new Settings.ProcessorSettings();
        settings2.className = "org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor";
        settings2.config = new HashMap<>();
        settings2.config.put("cacheExpirationTime", 600000);
        settings2.config.put("cacheMaxSize", 1000);
        this.janusGraphSettings.processors.add(settings1);
        this.janusGraphSettings.processors.add(settings2);

        this.janusGraphSettings.resultIterationBatchSize = DatabaseDescriptor.getJanusgraph_resultiterationbatchsize();


        Scriptengines janusgraph_scriptengines = DatabaseDescriptor.getJanusgraph_scriptengines();

        String[] split = janusgraph_scriptengines.gremlin_groovy_plugins.split(";");
        Map<String,Map<String, Object>> map=new HashMap<>();
        for (int i = 0; i < split.length; i++) {
            String[] sp = split[i].split("\\|");
            Map<String, Object> list=JSONObject.parseObject(sp[1],Map.class);
            map.put(sp[0],list);
        }



        Settings.ScriptEngineSettings script = new Settings.ScriptEngineSettings();
//        Map<String, Object> maps = new HashMap<>();
//        List<String> list1 = new ArrayList<>();
//        List<String> list2 = new ArrayList<>();
//        list1.add("java.lang.Math");
//        list2.add("java.lang.Math#*");
//        maps.put("classImports", list1);
//        maps.put("methodImports", list2);
        script.plugins = new HashMap<>();

        for (String k:map.keySet()){
            Map<String, Object> stringListMap = map.get(k);
            script.plugins.put(k.trim(),stringListMap);
        }
        //script.plugins.put("org.apache.tinkerpop.gremlin.jsr223.ImportGremlinPlugin", maps);
        this.janusGraphSettings.scriptEngines = new HashMap<>();
        this.janusGraphSettings.scriptEngines.put("gremlin-groovy", script);


        Settings.SerializerSettings serializerSettings1 = new Settings.SerializerSettings();
        serializerSettings1.className = "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1";
        List<String> ioRegistries = new ArrayList<>();
        ioRegistries.add("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry");
        serializerSettings1.config = new HashMap<>();
        serializerSettings1.config.put("ioRegistries", ioRegistries);

        Settings.SerializerSettings serializerSettings2 = new Settings.SerializerSettings();
        serializerSettings2.className = "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1";
        serializerSettings2.config = new HashMap<>();
        serializerSettings2.config.put("serializeResultToString", true);

        Settings.SerializerSettings serializerSettings3 = new Settings.SerializerSettings();
        serializerSettings3.className = "org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3";
        List<String> ioRegistries3 = new ArrayList<>();
        ioRegistries.add("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry");
        serializerSettings3.config = new HashMap<>();
        serializerSettings3.config.put("ioRegistries", ioRegistries3);

        this.janusGraphSettings.serializers = new ArrayList<>();
        this.janusGraphSettings.serializers.add(serializerSettings1);
        this.janusGraphSettings.serializers.add(serializerSettings2);
        this.janusGraphSettings.serializers.add(serializerSettings3);

        this.janusGraphSettings.sessionLifetimeTimeout = 600000;
        this.janusGraphSettings.strictTransactionManagement = false;
        this.janusGraphSettings.threadPoolBoss = 1;
        this.janusGraphSettings.threadPoolWorker = 1;
        this.janusGraphSettings.useCommonEngineForSessions = true;
        this.janusGraphSettings.useEpollEventLoop = false;
        this.janusGraphSettings.useGlobalFunctionCacheForSessions = true;
        this.janusGraphSettings.writeBufferHighWaterMark = DatabaseDescriptor.getJanusgraph_writebufferhighwatermark();
        this.janusGraphSettings.writeBufferLowWaterMark = DatabaseDescriptor.getJanusgraph_writebufferlowwatermark();
    }

    private static void configure(ServerGremlinExecutor serverGremlinExecutor) {
        GraphManager graphManager = serverGremlinExecutor.getGraphManager();
        if (graphManager instanceof JanusGraphManager) {
            ((JanusGraphManager) graphManager).configureGremlinExecutor(serverGremlinExecutor.getGremlinExecutor());
        }
    }

    public synchronized CompletableFuture<Void> stop() {
        if (this.gremlinServer == null) {
            return CompletableFuture.completedFuture((Void) null);
        } else if (this.serverStopped != null) {
            return this.serverStopped;
        } else {
            if (this.grpcServer != null) {
                this.grpcServer.shutdownNow();
            }

            this.serverStopped = this.gremlinServer.stop();
            return this.serverStopped;
        }
    }

    private static String getHeader() {
        String var10000 = System.lineSeparator();
        return "                                                                      " + var10000 + "   mmm                                mmm                       #     " + System.lineSeparator() + "     #   mmm   m mm   m   m   mmm   m\"   \"  m mm   mmm   mmmm   # mm  " + System.lineSeparator() + "     #  \"   #  #\"  #  #   #  #   \"  #   mm  #\"  \" \"   #  #\" \"#  #\"  # " + System.lineSeparator() + "     #  m\"\"\"#  #   #  #   #   \"\"\"m  #    #  #     m\"\"\"#  #   #  #   # " + System.lineSeparator() + " \"mmm\"  \"mm\"#  #   #  \"mm\"#  \"mmm\"   \"mmm\"  #     \"mm\"#  ##m#\"  #   # " + System.lineSeparator() + "                                                         #            " + System.lineSeparator() + "                                                         \"            " + System.lineSeparator();
    }

    private static void printHeader() {
        //logger.info(getHeader());
        logger.info("JanusGraph Version: {}", Manifests.read("janusgraphVersion"));
        logger.info("TinkerPop Version: {}", Manifests.read("tinkerpopVersion"));
    }


}
