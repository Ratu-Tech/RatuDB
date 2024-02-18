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

package org.opensearch.action.admin.indices.alias.get;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Transport response for listing index aliases
 *
 * @opensearch.internal
 */
public class GetAliasesResponse extends ActionResponse {

    private final Map<String, List<AliasMetadata>> aliases;

    public GetAliasesResponse(final Map<String, List<AliasMetadata>> aliases) {
        this.aliases = Collections.unmodifiableMap(aliases);
    }

    public GetAliasesResponse(StreamInput in) throws IOException {
        super(in);
        aliases = in.readMap(StreamInput::readString, i -> i.readList(AliasMetadata::new));
    }

    public Map<String, List<AliasMetadata>> getAliases() {
        return aliases;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(aliases, StreamOutput::writeString, StreamOutput::writeList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetAliasesResponse that = (GetAliasesResponse) o;
        return Objects.equals(aliases, that.aliases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliases);
    }
}
