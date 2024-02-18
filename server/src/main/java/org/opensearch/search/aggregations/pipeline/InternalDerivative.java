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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of derivitive pipeline
 *
 * @opensearch.internal
 */
public class InternalDerivative extends InternalSimpleValue implements Derivative {
    private final double normalizationFactor;

    InternalDerivative(String name, double value, double normalizationFactor, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, value, formatter, metadata);
        this.normalizationFactor = normalizationFactor;
    }

    /**
     * Read from a stream.
     */
    public InternalDerivative(StreamInput in) throws IOException {
        super(in);
        normalizationFactor = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeDouble(normalizationFactor);
    }

    @Override
    public String getWriteableName() {
        return DerivativePipelineAggregationBuilder.NAME;
    }

    @Override
    public double normalizedValue() {
        return normalizationFactor > 0 ? (value() / normalizationFactor) : value();
    }

    DocValueFormat formatter() {
        return format;
    }

    double getNormalizationFactor() {
        return normalizationFactor;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return value();
        } else if (path.size() == 1 && "normalized_value".equals(path.get(0))) {
            return normalizedValue();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);

        if (normalizationFactor > 0) {
            boolean hasValue = !(Double.isInfinite(normalizedValue()) || Double.isNaN(normalizedValue()));
            builder.field("normalized_value", hasValue ? normalizedValue() : null);
            if (hasValue && format != DocValueFormat.RAW) {
                builder.field("normalized_value_as_string", format.format(normalizedValue()));
            }
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), normalizationFactor, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalDerivative other = (InternalDerivative) obj;
        return Objects.equals(value, other.value) && Objects.equals(normalizationFactor, other.normalizationFactor);
    }
}
