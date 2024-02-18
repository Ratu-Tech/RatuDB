/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.index.mapper.DocCountFieldMapper;

import java.io.IOException;

/**
 * An implementation of a doc_count provider that reads the value
 * of the _doc_count field in the document. If a document does not have a
 * _doc_count field the implementation will return 1 as the default value.
 */
public class DocCountProvider {

    private NumericDocValues docCountValues;

    public long getDocCount(int doc) throws IOException {
        if (docCountValues != null && docCountValues.advanceExact(doc)) {
            return docCountValues.longValue();
        } else {
            return 1L;
        }
    }

    public void setLeafReaderContext(LeafReaderContext ctx) throws IOException {
        docCountValues = DocValues.getNumeric(ctx.reader(), DocCountFieldMapper.NAME);
    }
}
