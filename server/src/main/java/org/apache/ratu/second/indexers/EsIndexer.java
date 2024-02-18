/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratu.second.indexers;

import com.google.common.base.Stopwatch;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.ratu.second.ElasticSecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EsIndexer extends NoOpIndexer {

    private static final Logger logger = LoggerFactory.getLogger(EsIndexer.class);

    private final ElasticSecondaryIndex index;
    private final DecoratedKey key;
    private final int nowInSec;
    private final String id;
    private final boolean delete;

    private LinkedBlockingQueue<Row> rowLinkedBlockingQueue = new LinkedBlockingQueue<>();

    private final Boolean asyncWrite;

    public EsIndexer(ElasticSecondaryIndex index, DecoratedKey key, int nowInSec, boolean withDelete,Boolean asyncWrite) {
        this.key = key;
        this.nowInSec = nowInSec;
        this.index = index;
        this.id = ByteBufferUtil.bytesToHex(key.getKey());
        this.delete = withDelete;
        this.asyncWrite = asyncWrite;
    }


    @Override
    public void begin() {
    }


    @Override
    public void insertRow(Row row) {
        try {
            rowLinkedBlockingQueue.put(row);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateRow(Row oldRowData, Row newRowData) {
        try{
            rowLinkedBlockingQueue.put(newRowData);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void partitionDelete(DeletionTime deletionTime) {
        final Stopwatch time = Stopwatch.createStarted();
        index.delete(key);
        logger.debug("{} partitionDelete "+id+" took {}ms", index.index_name, time.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void finish() {
        commit();
    }

    @Override
    public void commit() {
        // 最后统一提交写入
        while (!rowLinkedBlockingQueue.isEmpty()) {
            index.index(this.key, rowLinkedBlockingQueue.poll(), null, nowInSec,asyncWrite);
        }
    }
}
