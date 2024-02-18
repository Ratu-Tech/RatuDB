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

package org.opensearch.search;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Since {@link org.opensearch.search.internal.SearchContext} no longer hold the states of search, the top K results
 * (i.e., documents that will be rescored by query rescorers) need to be serialized/ deserialized between search phases.
 * A {@link RescoreDocIds} encapsulates the top K results for each rescorer by its ordinal index.
 *
 * @opensearch.internal
 */
public final class RescoreDocIds implements Writeable {
    public static final RescoreDocIds EMPTY = new RescoreDocIds(Collections.emptyMap());

    private final Map<Integer, Set<Integer>> docIds;

    public RescoreDocIds(Map<Integer, Set<Integer>> docIds) {
        this.docIds = docIds;
    }

    public RescoreDocIds(StreamInput in) throws IOException {
        docIds = in.readMap(StreamInput::readVInt, i -> i.readSet(StreamInput::readVInt));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(docIds, StreamOutput::writeVInt, (o, v) -> o.writeCollection(v, StreamOutput::writeVInt));
    }

    public Set<Integer> getId(int index) {
        return docIds.get(index);
    }
}
