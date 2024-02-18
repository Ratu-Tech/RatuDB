/*
 * Copyright 2019 Genesys Telecommunications Laboratories, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratu.second.esclient;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.schema.TableMetadata;


public class SingleRowIterator extends AbstractUnfilteredRowIterator {

  private Unfiltered row;

  public SingleRowIterator(TableMetadata metadata, Unfiltered row, DecoratedKey key, RegularAndStaticColumns columns) {
    super(metadata, key, DeletionTime.LIVE, columns, Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS);
    this.row = row;
  }

  public SingleRowIterator(RowIterator partition, Unfiltered row) {
    super(partition.metadata(), partition.partitionKey(), DeletionTime.LIVE, partition.columns(), partition.staticRow(),
      partition.isReverseOrder(), EncodingStats.NO_STATS);
    this.row = row;
  }

  @Override
  protected synchronized Unfiltered computeNext() {
    try {
      return row == null ? endOfData() : row; //we have to return endOfData() when we're done
    } finally {
      row = null;
    }
  }
}
