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

package org.opensearch.action.admin.indices.cache.clear;

import org.opensearch.Version;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport request for clearing cache
 *
 * @opensearch.internal
 */
public class ClearIndicesCacheRequest extends BroadcastRequest<ClearIndicesCacheRequest> {

    private boolean queryCache = false;
    private boolean fieldDataCache = false;
    private boolean requestCache = false;
    private boolean fileCache = false;
    private String[] fields = Strings.EMPTY_ARRAY;

    public ClearIndicesCacheRequest(StreamInput in) throws IOException {
        super(in);
        queryCache = in.readBoolean();
        fieldDataCache = in.readBoolean();
        fields = in.readStringArray();
        requestCache = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_2_8_0)) {
            fileCache = in.readBoolean();
        }
    }

    public ClearIndicesCacheRequest(String... indices) {
        super(indices);
    }

    public boolean queryCache() {
        return queryCache;
    }

    public ClearIndicesCacheRequest queryCache(boolean queryCache) {
        this.queryCache = queryCache;
        return this;
    }

    public boolean requestCache() {
        return this.requestCache;
    }

    public ClearIndicesCacheRequest requestCache(boolean requestCache) {
        this.requestCache = requestCache;
        return this;
    }

    public boolean fieldDataCache() {
        return this.fieldDataCache;
    }

    public ClearIndicesCacheRequest fieldDataCache(boolean fieldDataCache) {
        this.fieldDataCache = fieldDataCache;
        return this;
    }

    public boolean fileCache() {
        return this.fileCache;
    }

    public ClearIndicesCacheRequest fileCache(boolean fileCache) {
        this.fileCache = fileCache;
        return this;
    }

    public ClearIndicesCacheRequest fields(String... fields) {
        this.fields = fields == null ? Strings.EMPTY_ARRAY : fields;
        return this;
    }

    public String[] fields() {
        return this.fields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(queryCache);
        out.writeBoolean(fieldDataCache);
        out.writeStringArrayNullable(fields);
        out.writeBoolean(requestCache);
        if (out.getVersion().onOrAfter(Version.V_2_8_0)) {
            out.writeBoolean(fileCache);
        }
    }
}
