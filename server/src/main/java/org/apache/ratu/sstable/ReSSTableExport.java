/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratu.sstable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.SSTableExport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ReSSTableExport {

    private static final Logger logger = LoggerFactory.getLogger(ReSSTableExport.class);


    private Thread syncDataHookThread;

    private static ExecutorService executorService = Executors.newFixedThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("SyncData" + thread.getId());
            return thread;
        }
    });

    public static void main(String[] args) {
        try {
            SSTableExport.main(args);


            Map<DecoratedKey, Row> poll = JsonTransformer.rows.poll();
            for (DecoratedKey key:poll.keySet()){
                String primaryKey = key.getPrimaryKey(SSTableExport.tableMetadataRef.get());
                String primaryKeyValue = key.getPrimaryKeyValue(SSTableExport.tableMetadataRef.get());
                Row value = poll.get(key);
//                ElasticSecondaryIndex index=new ElasticSecondaryIndex(columnFamilyStore,null);
//                index.index(key,value,null,0,false);
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("SSTableExport Data Exception:", e);
        }

        System.exit(0);
    }
}
