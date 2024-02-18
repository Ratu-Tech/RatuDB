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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * A TDigest percentiles rank agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedTDigestPercentileRanks extends ParsedPercentileRanks {

    @Override
    public String getType() {
        return InternalTDigestPercentileRanks.NAME;
    }

    @Override
    public Iterator<Percentile> iterator() {
        final Iterator<Percentile> iterator = super.iterator();
        return new Iterator<Percentile>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Percentile next() {
                Percentile percentile = iterator.next();
                return new Percentile(percentile.getValue(), percentile.getPercent());
            }
        };
    }

    private static final ObjectParser<ParsedTDigestPercentileRanks, Void> PARSER = new ObjectParser<>(
        ParsedTDigestPercentileRanks.class.getSimpleName(),
        true,
        ParsedTDigestPercentileRanks::new
    );
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedTDigestPercentileRanks fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTDigestPercentileRanks aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
