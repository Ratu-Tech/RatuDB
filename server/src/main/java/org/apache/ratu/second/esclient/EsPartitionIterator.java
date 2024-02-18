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

package org.apache.ratu.second.esclient;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.ratu.second.ElasticSecondaryIndex;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;


public class EsPartitionIterator implements UnfilteredPartitionIterator {


    private static final String FAKE_METADATA = "{\"metadata\":\"none\"}";

    private static final String ES_HITS = "hits";
    private static final String ES_SOURCE = "_source";

    private final Iterator<SearchResultRow> esResultIterator;
    private final ColumnFamilyStore baseCfs;
    private final ReadCommand command;

    private final ElasticSecondaryIndex index;

    private final List<String> partitionKeysNames;

    private final JSONObject aggsResult;


    public EsPartitionIterator(ElasticSecondaryIndex index, SearchResult searchResult, List<String> partitionKeysNames, ReadCommand command, String searchId, JSONObject object) {
        this.baseCfs = index.baseCfs;

        this.esResultIterator = searchResult.items.iterator();
        this.command = command;
        this.index = index;
        this.partitionKeysNames = partitionKeysNames;
        this.aggsResult= object;
        Tracing.trace("ESI {} FakePartitionIterator initialized", searchId);
    }

    @Override
    public void close() {

    }

    @Override
    public TableMetadata metadata() {
        return command.metadata();
    }

    @Override
    public boolean hasNext() {
        return esResultIterator.hasNext();
    }

    @Override
    public UnfilteredRowIterator next() {

        if (!esResultIterator.hasNext()) {
            return null;
        }

        SearchResultRow esResult = esResultIterator.next();

        JSONObject jsonMetadata = esResult.docMetadata;

        //And PK value
        DecoratedKey partitionKey = baseCfs.getPartitioner().decorateKey(esResult.partitionKey);


        ColumnMetadata columnMetadata = ColumnMetadata.regularColumn(this.baseCfs.metadata(), ByteBufferUtil.bytes("aggs"), UTF8Type.instance);

        TableMetadata.Builder tableBuild=this.baseCfs.metadata().unbuild();
        //tableBuild.addColumn(columnMetadata);
        TableMetadata build = tableBuild.build();




        SinglePartitionReadCommand readCommand = SinglePartitionReadCommand.create(
                build,
                command.nowInSec(),
                command.columnFilter(),
                RowFilter.NONE,
                DataLimits.NONE,
                partitionKey,
                command.clusteringIndexFilter(partitionKey));


        PartitionIterator partition =
                StorageProxy.read(SinglePartitionReadCommand.Group.one(readCommand), ConsistencyLevel.ALL, System.nanoTime());


        Row next = partition.next().next();

        // 以下新增，分割线
        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();

        rowBuilder.newRow(next.clustering());  //need to be first
        rowBuilder.addPrimaryKeyLivenessInfo(next.primaryKeyLivenessInfo());
        rowBuilder.addRowDeletion(next.deletion());

        // 判断是否显示聚合的结果，要是不显示聚合结果，就显示es取回来的原始数据
        JSONObject result = null;
        if (!aggsResult.isEmpty()){
            result=aggsResult;
        }else {
            result = jsonMetadata;
        }

        if (!this.baseCfs.metadata().columns().contains(columnMetadata)){
            columnMetadata = this.baseCfs.metadata().columns().iterator().next();
        }

        boolean primaryKeyKind = columnMetadata.kind.isPrimaryKeyKind();
        if (primaryKeyKind) {
            ImmutableList<ColumnMetadata> cMetadata = this.baseCfs.metadata().columns().asList();
            for (int i = 0; i < cMetadata.size() ; i++) {
                ColumnMetadata  metadata= cMetadata.get(i);
                if (!metadata.name.equals(columnMetadata.name)){
                    columnMetadata = metadata;
                    break;
                }
            }
        }


        //add metadata cell
        ByteBuffer value = ByteBufferUtil.bytes(result.toString(), UTF_8);

        BufferCell metadataCell = BufferCell.live(columnMetadata, System.currentTimeMillis(), value);
        rowBuilder.addCell(metadataCell);
        //copy existing cells
        next.cells().forEach(cell -> {
            if (!index.indexColumnName.equals(cell.column().name.toString())) {
                rowBuilder.addCell(cell);
            }
        });

        next = rowBuilder.build();

        return new SingleRowIterator(build, next, partitionKey, build.regularAndStaticColumns());
    }
}
