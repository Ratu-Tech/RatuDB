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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.segments;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.Segment;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Collection of shard segments
 *
 * @opensearch.internal
 */
public class ShardSegments implements Writeable, Iterable<Segment> {

    private final ShardRouting shardRouting;

    private final List<Segment> segments;

    ShardSegments(ShardRouting shardRouting, List<Segment> segments) {
        this.shardRouting = shardRouting;
        this.segments = segments;
    }

    ShardSegments(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        segments = in.readList(Segment::new);
    }

    @Override
    public Iterator<Segment> iterator() {
        return segments.iterator();
    }

    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public List<Segment> getSegments() {
        return this.segments;
    }

    public int getNumberOfCommitted() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isCommitted()) {
                count++;
            }
        }
        return count;
    }

    public int getNumberOfSearch() {
        int count = 0;
        for (Segment segment : segments) {
            if (segment.isSearch()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        out.writeList(segments);
    }
}
