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

package org.opensearch.search.profile.query;

import org.opensearch.Version;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Public interface and serialization container for profiled timings of the
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 *
 * @opensearch.internal
 */
public class CollectorResult implements ToXContentObject, Writeable {

    public static final String REASON_SEARCH_COUNT = "search_count";
    public static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    public static final String REASON_SEARCH_TERMINATE_AFTER_COUNT = "search_terminate_after_count";
    public static final String REASON_SEARCH_POST_FILTER = "search_post_filter";
    public static final String REASON_SEARCH_MIN_SCORE = "search_min_score";
    public static final String REASON_SEARCH_MULTI = "search_multi";
    public static final String REASON_AGGREGATION = "aggregation";
    public static final String REASON_AGGREGATION_GLOBAL = "aggregation_global";
    public static final String COLLECTOR_MANAGER = "CollectorManager";

    private static final ParseField NAME = new ParseField("name");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField TIME = new ParseField("time");
    private static final ParseField TIME_NANOS = new ParseField("time_in_nanos");
    private static final ParseField REDUCE_TIME_NANOS = new ParseField("reduce_time_in_nanos");
    private static final ParseField MAX_SLICE_TIME_NANOS = new ParseField("max_slice_time_in_nanos");
    private static final ParseField MIN_SLICE_TIME_IN_NANOS = new ParseField("min_slice_time_in_nanos");
    private static final ParseField AVG_SLICE_TIME_IN_NANOS = new ParseField("avg_slice_time_in_nanos");
    private static final ParseField SLICE_COUNT = new ParseField("slice_count");
    private static final ParseField CHILDREN = new ParseField("children");

    /**
     * A more friendly representation of the Collector's class name
     */
    private final String collectorName;

    /**
     * A "hint" to help provide some context about this Collector
     */
    private final String reason;

    /**
     * The total elapsed time for this Collector
     */
    private final long time;

    /**
     * The total elapsed time in reduce phase for this CollectorManager
     */
    private final long reduceTime;

    /**
     * The maximum slice time for this CollectorManager
     */
    private final long maxSliceTime;

    /**
     * The minimum slice time for this CollectorManager
     */
    private final long minSliceTime;

    /**
     * The average slice time for this CollectorManager
     */
    private final long avgSliceTime;

    /**
     * The segment slice count for this CollectorManager
     */
    private final int sliceCount;

    /**
     * A list of children collectors "embedded" inside this collector
     */
    private List<CollectorResult> children;

    public CollectorResult(String collectorName, String reason, long time, List<CollectorResult> children) {
        this(collectorName, reason, time, 0L, time, time, time, 1, children);
    }

    public CollectorResult(
        String collectorName,
        String reason,
        long time,
        long reduceTime,
        long maxSliceTime,
        long minSliceTime,
        long avgSliceTime,
        int sliceCount,
        List<CollectorResult> children
    ) {
        this.collectorName = collectorName;
        this.reason = reason;
        this.time = time;
        this.reduceTime = reduceTime;
        this.maxSliceTime = maxSliceTime;
        this.minSliceTime = minSliceTime;
        this.avgSliceTime = avgSliceTime;
        this.sliceCount = sliceCount;
        this.children = children;
    }

    /**
     * Read from a stream.
     */
    public CollectorResult(StreamInput in) throws IOException {
        this.collectorName = in.readString();
        this.reason = in.readString();
        this.time = in.readLong();
        int size = in.readVInt();
        this.children = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            CollectorResult child = new CollectorResult(in);
            this.children.add(child);
        }
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            this.reduceTime = in.readLong();
            this.maxSliceTime = in.readLong();
            this.minSliceTime = in.readLong();
            this.avgSliceTime = in.readLong();
            this.sliceCount = in.readVInt();
        } else {
            this.reduceTime = 0L;
            this.maxSliceTime = this.time;
            this.minSliceTime = this.time;
            this.avgSliceTime = this.time;
            this.sliceCount = 1;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(collectorName);
        out.writeString(reason);
        out.writeLong(time);
        out.writeVInt(children.size());
        for (CollectorResult child : children) {
            child.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeLong(reduceTime);
            out.writeLong(maxSliceTime);
            out.writeLong(minSliceTime);
            out.writeLong(avgSliceTime);
            out.writeVInt(sliceCount);
        }
    }

    /**
     * @return the profiled time for this collector/collector manager (inclusive of children)
     */
    public long getTime() {
        return this.time;
    }

    /**
     * @return the profiled reduce time for this collector manager (inclusive of children)
     */
    public long getReduceTime() {
        return this.reduceTime;
    }

    /**
     * @return the profiled maximum slice time for this collector manager (inclusive of children)
     */
    public long getMaxSliceTime() {
        return this.maxSliceTime;
    }

    /**
     * @return the profiled minimum slice time for this collector manager (inclusive of children)
     */
    public long getMinSliceTime() {
        return this.minSliceTime;
    }

    /**
     * @return the profiled average slice time for this collector manager (inclusive of children)
     */
    public long getAvgSliceTime() {
        return this.avgSliceTime;
    }

    /**
     * @return the profiled segment slice count for this collector manager (inclusive of children)
     */
    public int getSliceCount() {
        return this.sliceCount;
    }

    /**
     * @return a human readable "hint" about what this collector/collector manager was used for
     */
    public String getReason() {
        return this.reason;
    }

    /**
     * @return the lucene class name of the collector/collector manager
     */
    public String getName() {
        return this.collectorName;
    }

    /**
     * @return a list of children collectors/collector managers
     */
    public List<CollectorResult> getProfiledChildren() {
        return children;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder = builder.startObject();
        builder.field(NAME.getPreferredName(), getName());
        builder.field(REASON.getPreferredName(), getReason());
        if (builder.humanReadable()) {
            builder.field(TIME.getPreferredName(), new TimeValue(getTime(), TimeUnit.NANOSECONDS).toString());
        }
        builder.field(TIME_NANOS.getPreferredName(), getTime());
        if (getName().contains(COLLECTOR_MANAGER)) {
            builder.field(REDUCE_TIME_NANOS.getPreferredName(), getReduceTime());
            builder.field(MAX_SLICE_TIME_NANOS.getPreferredName(), getMaxSliceTime());
            builder.field(MIN_SLICE_TIME_IN_NANOS.getPreferredName(), getMinSliceTime());
            builder.field(AVG_SLICE_TIME_IN_NANOS.getPreferredName(), getAvgSliceTime());
            builder.field(SLICE_COUNT.getPreferredName(), getSliceCount());
        }

        if (!children.isEmpty()) {
            builder = builder.startArray(CHILDREN.getPreferredName());
            for (CollectorResult child : children) {
                builder = child.toXContent(builder, params);
            }
            builder = builder.endArray();
        }
        builder = builder.endObject();
        return builder;
    }

    public static CollectorResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        String name = null, reason = null;
        long time = -1;
        long reduceTime = -1;
        long maxSliceTime = -1;
        long minSliceTime = -1;
        long avgSliceTime = -1;
        int sliceCount = 0;
        List<CollectorResult> children = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    name = parser.text();
                } else if (REASON.match(currentFieldName, parser.getDeprecationHandler())) {
                    reason = parser.text();
                } else if (TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    // we need to consume this value, but we use the raw nanosecond value
                    parser.text();
                } else if (TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else if (REDUCE_TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    reduceTime = parser.longValue();
                } else if (MAX_SLICE_TIME_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxSliceTime = parser.longValue();
                } else if (MIN_SLICE_TIME_IN_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    minSliceTime = parser.longValue();
                } else if (AVG_SLICE_TIME_IN_NANOS.match(currentFieldName, parser.getDeprecationHandler())) {
                    avgSliceTime = parser.longValue();
                } else if (SLICE_COUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                    sliceCount = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (CHILDREN.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        children.add(CollectorResult.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new CollectorResult(name, reason, time, reduceTime, maxSliceTime, minSliceTime, avgSliceTime, sliceCount, children);
    }
}
