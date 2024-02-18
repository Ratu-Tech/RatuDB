/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.common.settings.Settings;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class NodeSetting {


    /**
     * 获取seeds 配置 转换 opensearch 配置
     *
     * @param filePath 配置问价路径
     * @return 转换好的 opensearch 配置
     */
    private static String getSeedsConfig(String filePath) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(new File(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Yaml yaml = new Yaml();
        Map<String, List<Map<String, List<Map<String, Object>>>>> data = yaml.load(inputStream);
        String seedProvider = data.get("seed_provider").get(0).get("parameters").get(0).get("seeds").toString();
        String[] split = seedProvider.split(",");
        String[] ipArr = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            String substring = split[i].substring(0, split[i].indexOf(":"));
            ipArr[i] = substring;
        }
        return JSON.toJSONString(ipArr).replace("[", "").replace("]", "").replace("\"", "").trim();
    }


    public static String getCassandraYamlByKey(String key, String path) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(new File(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        Yaml yaml = new Yaml();
        Map<String, Object> data = yaml.load(inputStream);
        if (data.get(key) == null) {
            return null;
        }
        return data.get(key).toString();
    }

    public static Settings nodeSettings(String dataPath, Settings settings, String path, SnitchProperties snitchProperties) {
        //配置角色
        String opensearchRole = getCassandraYamlByKey("node_roles", path);
        Settings.Builder builder = Settings.builder();
        if (!StringUtils.isBlank(opensearchRole)) {
            builder.put("node.roles", opensearchRole);
        }

        //配置存储目录
        String dataFileDirectories = getCassandraYamlByKey("data_file_directories", path);
        if (StringUtils.isBlank(dataFileDirectories)) {
            dataFileDirectories = dataPath;
        } else {
            dataFileDirectories = dataFileDirectories.replace("[", "").replace("]", "");
        }
        Path opensearchDataPath = Path.of(dataFileDirectories + "/search");
        System.setProperty("opensearch.data.path", dataFileDirectories);

        String initialClusterManagerNodes = getCassandraYamlByKey("manager_nodes", path);
        if (StringUtils.isBlank(initialClusterManagerNodes)) {
            initialClusterManagerNodes = getSeedsConfig(path);
        }

        //Cassandra nodetool 工具端口
        System.setProperty("cassandra.jmx.local.port", "7199");
        System.setProperty("cassandra.jmx.remote.port", "7199");
        if (getSeedsConfig(path).equals("127.0.0.1") || getSeedsConfig(path).equals("localhost")) {
            builder.put("network.host", getCassandraYamlByKey("rpc_address", path));
            builder.put("http.port", getCassandraYamlByKey("opensearch_httpport", path));
            builder.put("transport.port", getCassandraYamlByKey("opensearch_transportport", path));
            builder.put("node.name", getCassandraYamlByKey("rpc_address", path));
            //builder.put("discovery.seed_hosts", getSeedsConfig(path));
            builder.put("cluster.name", getCassandraYamlByKey("cluster_name", path));
            builder.put("path.home", settings.get("path.home"));
            builder.put("path.data", opensearchDataPath);
            //builder.put("discovery.type", "single-node");
            builder.put("cluster.initial_cluster_manager_nodes",initialClusterManagerNodes);
            builder.put("node.attr.rack_id", snitchProperties.get("dc").trim() + "-" + snitchProperties.get("rack").trim());
            //缓冲区占用，内存的30%，默认10%
            //builder.put("indices.memory.index_buffer_size", "50%");
            //父级别断路器，会达到实际内存的95% 处罚。
            //builder.put("indices.breaker.total.use_real_memory", "false");
            //builder.put("action.destructive_requires_name", "true");
            //builder.put("transport.tcp.compress", "true");
            return builder.build();
        }

        builder.put("network.host", getCassandraYamlByKey("rpc_address", path));
        builder.put("http.port", getCassandraYamlByKey("opensearch_httpport", path));
        builder.put("transport.port", getCassandraYamlByKey("opensearch_transportport", path));
        builder.put("node.name", getCassandraYamlByKey("rpc_address", path));
        builder.put("discovery.seed_hosts", getSeedsConfig(path));
        builder.put("cluster.initial_cluster_manager_nodes", initialClusterManagerNodes);
        builder.put("cluster.name", getCassandraYamlByKey("cluster_name", path));
        builder.put("path.home", settings.get("path.home"));
        builder.put("path.data", opensearchDataPath);
        builder.put("node.attr.rack_id", snitchProperties.get("dc").trim() + "-" + snitchProperties.get("rack").trim());
        //缓冲区占用，内存的30%，默认10%
        //builder.put("indices.memory.index_buffer_size", "10%");
        //父级别断路器，会达到实际内存的95% 处罚。
        //builder.put("indices.breaker.total.use_real_memory", "false");
        //builder.put("action.destructive_requires_name", "true");
        //builder.put("transport.tcp.compress", "true");
        return builder.build();
    }
}
