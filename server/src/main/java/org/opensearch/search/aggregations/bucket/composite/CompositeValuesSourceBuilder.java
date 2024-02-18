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

package org.opensearch.search.aggregations.bucket.composite;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.opensearch.search.aggregations.bucket.missing.MissingOrder.fromString;

/**
 * A {@link ValuesSource} builder for {@link CompositeAggregationBuilder}
 *
 * @opensearch.internal
 */
public abstract class CompositeValuesSourceBuilder<AB extends CompositeValuesSourceBuilder<AB>> implements Writeable, ToXContentFragment {

    protected final String name;
    private String field = null;
    private Script script = null;
    private ValueType userValueTypeHint = null;
    private boolean missingBucket = false;
    private MissingOrder missingOrder = MissingOrder.DEFAULT;
    private SortOrder order = SortOrder.ASC;
    private String format = null;

    public CompositeValuesSourceBuilder(String name) {
        this.name = name;
    }

    public CompositeValuesSourceBuilder(StreamInput in) throws IOException {
        this.name = in.readString();
        this.field = in.readOptionalString();
        if (in.readBoolean()) {
            this.script = new Script(in);
        }
        if (in.readBoolean()) {
            this.userValueTypeHint = ValueType.readFromStream(in);
        }
        this.missingBucket = in.readBoolean();
        this.missingOrder = MissingOrder.readFromStream(in);
        this.order = SortOrder.readFromStream(in);
        this.format = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(field);
        boolean hasScript = script != null;
        out.writeBoolean(hasScript);
        if (hasScript) {
            script.writeTo(out);
        }
        boolean hasValueType = userValueTypeHint != null;
        out.writeBoolean(hasValueType);
        if (hasValueType) {
            userValueTypeHint.writeTo(out);
        }
        out.writeBoolean(missingBucket);
        missingOrder.writeTo(out);
        order.writeTo(out);
        out.writeOptionalString(format);
        innerWriteTo(out);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    protected abstract void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(type());
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        builder.field("missing_bucket", missingBucket);
        if (userValueTypeHint != null) {
            builder.field("value_type", userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (MissingOrder.isDefault(missingOrder) == false) {
            builder.field(MissingOrder.NAME, missingOrder.toString());
        }
        builder.field("order", order);
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, missingBucket, script, userValueTypeHint, order, format);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        @SuppressWarnings("unchecked")
        AB that = (AB) o;
        return Objects.equals(field, that.field())
            && Objects.equals(script, that.script())
            && Objects.equals(userValueTypeHint, that.userValuetypeHint())
            && Objects.equals(missingBucket, that.missingBucket())
            && Objects.equals(missingOrder, that.missingOrder())
            && Objects.equals(order, that.order())
            && Objects.equals(format, that.format());
    }

    public String name() {
        return name;
    }

    protected abstract String type();

    /**
     * Sets the field to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null");
        }
        this.field = field;
        return (AB) this;
    }

    /**
     * Gets the field to use for this source
     */
    public String field() {
        return field;
    }

    /**
     * Sets the script to use for this source
     */
    @SuppressWarnings("unchecked")
    public AB script(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("[script] must not be null");
        }
        this.script = script;
        return (AB) this;
    }

    /**
     * Gets the script to use for this source
     */
    public Script script() {
        return script;
    }

    /**
     * Sets the {@link ValueType} for the value produced by this source
     */
    @SuppressWarnings("unchecked")
    public AB userValuetypeHint(ValueType valueType) {
        if (valueType == null) {
            throw new IllegalArgumentException("[userValueTypeHint] must not be null");
        }
        this.userValueTypeHint = valueType;
        return (AB) this;
    }

    /**
     * Gets the {@link ValueType} for the value produced by this source
     */
    public ValueType userValuetypeHint() {
        return userValueTypeHint;
    }

    /**
     * If <code>true</code> an explicit <code>null</code> bucket will represent documents with missing values.
     */
    @SuppressWarnings("unchecked")
    public AB missingBucket(boolean missingBucket) {
        this.missingBucket = missingBucket;
        return (AB) this;
    }

    /**
     * False if documents with missing values are ignored, otherwise missing values are
     * represented by an explicit `null` value.
     */
    public boolean missingBucket() {
        return missingBucket;
    }

    /**
     * Sets the {@link MissingOrder} to use to order missing value.
     */
    public AB missingOrder(MissingOrder missingOrder) {
        this.missingOrder = missingOrder;
        return (AB) this;
    }

    /**
     * Sets the {@link MissingOrder} to use to order missing value.
     * @param missingOrder "first", "last" or "default".
     */
    public AB missingOrder(String missingOrder) {
        return missingOrder(fromString(missingOrder));
    }

    /**
     * Missing value order. {@link MissingOrder}.
     */
    public MissingOrder missingOrder() {
        return missingOrder;
    }

    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(String order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = SortOrder.fromString(order);
        return (AB) this;
    }

    /**
     * Sets the {@link SortOrder} to use to sort values produced this source
     */
    @SuppressWarnings("unchecked")
    public AB order(SortOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null");
        }
        this.order = order;
        return (AB) this;
    }

    /**
     * Gets the {@link SortOrder} to use to sort values produced this source
     */
    public SortOrder order() {
        return order;
    }

    /**
     * Sets the format to use for the output of the aggregation.
     */
    public AB format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return (AB) this;
    }

    /**
     * Gets the format to use for the output of the aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Creates a {@link CompositeValuesSourceConfig} for this source.
     *  @param queryShardContext   The shard context for this source.
     * @param config    The {@link ValuesSourceConfig} for this source.
     */
    protected abstract CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config)
        throws IOException;

    protected abstract ValuesSourceType getDefaultValuesSourceType();

    public final CompositeValuesSourceConfig build(QueryShardContext queryShardContext) throws IOException {
        if (missingBucket == false && missingOrder != MissingOrder.DEFAULT) {
            throw new IllegalArgumentException(MissingOrder.NAME + " require missing_bucket is true");
        }
        ValuesSourceConfig config = ValuesSourceConfig.resolve(
            queryShardContext,
            userValueTypeHint,
            field,
            script,
            null,
            timeZone(),
            format,
            getDefaultValuesSourceType()
        );
        return innerBuild(queryShardContext, config);
    }

    /**
     * The time zone for this value source. Default implementation returns {@code null}
     * because most value source types don't support time zone.
     */
    protected ZoneId timeZone() {
        return null;
    }
}
