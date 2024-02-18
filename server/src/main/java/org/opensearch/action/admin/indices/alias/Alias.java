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

package org.opensearch.action.admin.indices.alias;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Represents an alias, to be associated with an index
 *
 * @opensearch.internal
 */
public class Alias implements Writeable, ToXContentFragment {

    private static final ParseField FILTER = new ParseField("filter");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField INDEX_ROUTING = new ParseField("index_routing", "indexRouting", "index-routing");
    private static final ParseField SEARCH_ROUTING = new ParseField("search_routing", "searchRouting", "search-routing");
    private static final ParseField IS_WRITE_INDEX = new ParseField("is_write_index");
    private static final ParseField IS_HIDDEN = new ParseField("is_hidden");

    private String name;

    @Nullable
    private String filter;

    @Nullable
    private String indexRouting;

    @Nullable
    private String searchRouting;

    @Nullable
    private Boolean writeIndex;

    @Nullable
    private Boolean isHidden;

    public Alias(StreamInput in) throws IOException {
        name = in.readString();
        filter = in.readOptionalString();
        indexRouting = in.readOptionalString();
        searchRouting = in.readOptionalString();
        writeIndex = in.readOptionalBoolean();
        isHidden = in.readOptionalBoolean();
    }

    public Alias(String name) {
        this.name = name;
    }

    /**
     * Returns the alias name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the filter associated with the alias
     */
    public String filter() {
        return filter;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(String filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(Map<String, Object> filter) {
        if (filter == null || filter.isEmpty()) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
            builder.map(filter);
            this.filter = builder.toString();
            return this;
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to generate [" + filter + "]", e);
        }
    }

    /**
     * Associates a filter to the alias
     */
    public Alias filter(QueryBuilder filterBuilder) {
        if (filterBuilder == null) {
            this.filter = null;
            return this;
        }
        try {
            XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
            filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.close();
            this.filter = builder.toString();
            return this;
        } catch (IOException e) {
            throw new OpenSearchGenerationException("Failed to build json for alias request", e);
        }
    }

    /**
     * Associates a routing value to the alias
     */
    public Alias routing(String routing) {
        this.indexRouting = routing;
        this.searchRouting = routing;
        return this;
    }

    /**
     * Returns the index routing value associated with the alias
     */
    public String indexRouting() {
        return indexRouting;
    }

    /**
     * Associates an index routing value to the alias
     */
    public Alias indexRouting(String indexRouting) {
        this.indexRouting = indexRouting;
        return this;
    }

    /**
     * Returns the search routing value associated with the alias
     */
    public String searchRouting() {
        return searchRouting;
    }

    /**
     * Associates a search routing value to the alias
     */
    public Alias searchRouting(String searchRouting) {
        this.searchRouting = searchRouting;
        return this;
    }

    /**
     * @return the write index flag for the alias
     */
    public Boolean writeIndex() {
        return writeIndex;
    }

    /**
     *  Sets whether an alias is pointing to a write-index
     */
    public Alias writeIndex(@Nullable Boolean writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    /**
     * @return whether this alias is hidden or not
     */
    public Boolean isHidden() {
        return isHidden;
    }

    /**
     * Sets whether this alias is hidden
     */
    public Alias isHidden(@Nullable Boolean isHidden) {
        this.isHidden = isHidden;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(filter);
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
        out.writeOptionalBoolean(writeIndex);
        out.writeOptionalBoolean(isHidden);
    }

    /**
     * Parses an alias and returns its parsed representation
     */
    public static Alias fromXContent(XContentParser parser) throws IOException {
        Alias alias = new Alias(parser.currentName());

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        if (token == null) {
            throw new IllegalArgumentException("No alias is specified");
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (FILTER.match(currentFieldName, parser.getDeprecationHandler())) {
                    Map<String, Object> filter = parser.mapOrdered();
                    alias.filter(filter);
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.routing(parser.text());
                } else if (INDEX_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.indexRouting(parser.text());
                } else if (SEARCH_ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.searchRouting(parser.text());
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (IS_WRITE_INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.writeIndex(parser.booleanValue());
                } else if (IS_HIDDEN.match(currentFieldName, parser.getDeprecationHandler())) {
                    alias.isHidden(parser.booleanValue());
                }
            }
        }
        return alias;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);

        if (filter != null) {
            try (InputStream stream = new BytesArray(filter).streamInput()) {
                builder.rawField(FILTER.getPreferredName(), stream, MediaTypeRegistry.JSON);
            }
        }

        if (indexRouting != null && indexRouting.equals(searchRouting)) {
            builder.field(ROUTING.getPreferredName(), indexRouting);
        } else {
            if (indexRouting != null) {
                builder.field(INDEX_ROUTING.getPreferredName(), indexRouting);
            }
            if (searchRouting != null) {
                builder.field(SEARCH_ROUTING.getPreferredName(), searchRouting);
            }
        }

        builder.field(IS_WRITE_INDEX.getPreferredName(), writeIndex);

        if (isHidden != null) {
            builder.field(IS_HIDDEN.getPreferredName(), isHidden);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alias alias = (Alias) o;

        if (name != null ? !name.equals(alias.name) : alias.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
