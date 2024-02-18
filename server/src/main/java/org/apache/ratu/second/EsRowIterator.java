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

package org.apache.ratu.second;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;


public class EsRowIterator implements RowIterator {


    private final TableMetadata metadata;
    private final DecoratedKey partitionKey;
    private final RegularAndStaticColumns columns;
    private final Row staticRow;
    private final RowIterator singleIterator;

    private final RowIterator singleIteratorCopy;


    public EsRowIterator(RowIterator rowIterator,RowIterator rowIteratorCopy) {
        this.metadata = rowIterator.metadata();
        this.partitionKey = rowIterator.partitionKey();
        this.columns = rowIterator.columns();
        this.staticRow = rowIterator.staticRow();
        this.singleIterator = rowIterator;
        this.singleIteratorCopy = rowIteratorCopy;
    }


    @Override
    public TableMetadata metadata() {
        return this.metadata;
    }

    @Override
    public boolean isReverseOrder() {
        return false;
    }

    @Override
    public RegularAndStaticColumns columns() {
        return this.columns;
    }

    @Override
    public DecoratedKey partitionKey() {
        return this.partitionKey;
    }

    @Override
    public Row staticRow() {
        return this.staticRow;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasNext() {
        return this.singleIterator.hasNext();
    }

    @Override
    public Row next() {
        return this.singleIteratorCopy.next();
    }
}
