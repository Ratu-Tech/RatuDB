/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratu;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cassandra {

    private static final Logger logger = LoggerFactory.getLogger(Cassandra.class);

    public static void active() {
        String cassandraHome = System.getProperty("opensearch.path.home");
        String cassandraConfig=System.getProperty("opensearch.path.conf");
        System.setProperty("cassandra.config", "file://"+cassandraConfig+"/cassandra.yaml");
        System.setProperty("cassandra.storagedir", System.getProperty("opensearch.data.path"));
        System.setProperty("cassandra.home",cassandraHome);
        System.setProperty("cassandra.logdir",System.getProperty("opensearch.logs.base_path"));
        System.setProperty("java.library.path",cassandraHome+"/lib/sigar-bin");

        try {
            org.apache.cassandra.service.CassandraDaemon daemon = new org.apache.cassandra.service.CassandraDaemon();
            daemon.activate();
        } catch (Exception e) {
            logger.error("Cassandra 启动错误:",e);
        }
    }
}
