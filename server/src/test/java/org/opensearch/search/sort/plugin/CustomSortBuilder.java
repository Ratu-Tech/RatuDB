/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort.plugin;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortFieldAndFormat;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Custom sort builder that just rewrites to a basic field sort
 */
public class CustomSortBuilder extends SortBuilder<CustomSortBuilder> {
    public static String NAME = "_custom";
    public static ParseField SORT_FIELD = new ParseField("sort_field");

    public final String field;
    public final SortOrder order;

    public CustomSortBuilder(String field, SortOrder order) {
        this.field = field;
        this.order = order;
    }

    public CustomSortBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.order = in.readOptionalWriteable(SortOrder::readFromStream);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalWriteable(order);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SortBuilder<?> rewrite(final QueryRewriteContext ctx) throws IOException {
        return SortBuilders.fieldSort(field).order(order);
    }

    @Override
    protected SortFieldAndFormat build(final QueryShardContext context) throws IOException {
        throw new IllegalStateException("rewrite");
    }

    @Override
    public BucketedSort buildBucketedSort(final QueryShardContext context, final int bucketSize, final BucketedSort.ExtraData extra)
        throws IOException {
        throw new IllegalStateException("rewrite");
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        CustomSortBuilder other = (CustomSortBuilder) object;
        return Objects.equals(field, other.field) && Objects.equals(order, other.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, order);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SORT_FIELD.getPreferredName(), field);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static CustomSortBuilder fromXContent(XContentParser parser, String elementName) {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<CustomSortBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new CustomSortBuilder((String) a[0], (SortOrder) a[1])
    );

    static {
        PARSER.declareField(constructorArg(), XContentParser::text, SORT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> SortOrder.fromString(p.text()), ORDER_FIELD, ObjectParser.ValueType.STRING);
    }
}
