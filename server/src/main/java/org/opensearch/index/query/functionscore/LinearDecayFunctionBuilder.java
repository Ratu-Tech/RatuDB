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

package org.opensearch.index.query.functionscore;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.function.Functions;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Foundation builder for a linear decay
 *
 * @opensearch.internal
 */
public class LinearDecayFunctionBuilder extends DecayFunctionBuilder<LinearDecayFunctionBuilder> {
    public static final String NAME = "linear";
    public static final ScoreFunctionParser<LinearDecayFunctionBuilder> PARSER = new DecayFunctionParser<>(LinearDecayFunctionBuilder::new);
    public static final DecayFunction LINEAR_DECAY_FUNCTION = new LinearDecayScoreFunction();

    public LinearDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset) {
        super(fieldName, origin, scale, offset);
    }

    public LinearDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, @Nullable String functionName) {
        super(fieldName, origin, scale, offset, functionName);
    }

    public LinearDecayFunctionBuilder(String fieldName, Object origin, Object scale, Object offset, double decay) {
        super(fieldName, origin, scale, offset, decay);
    }

    public LinearDecayFunctionBuilder(
        String fieldName,
        Object origin,
        Object scale,
        Object offset,
        double decay,
        @Nullable String functionName
    ) {
        super(fieldName, origin, scale, offset, decay, functionName);
    }

    LinearDecayFunctionBuilder(String fieldName, BytesReference functionBytes) {
        super(fieldName, functionBytes);
    }

    /**
     * Read from a stream.
     */
    public LinearDecayFunctionBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public DecayFunction getDecayFunction() {
        return LINEAR_DECAY_FUNCTION;
    }

    /**
     * Linear decay
     *
     * @opensearch.internal
     */
    private static final class LinearDecayScoreFunction implements DecayFunction {

        @Override
        public double evaluate(double value, double scale) {
            return Math.max(0.0, (scale - value) / scale);
        }

        @Override
        public Explanation explainFunction(String valueExpl, double value, double scale, @Nullable String functionName) {
            return Explanation.match(
                (float) evaluate(value, scale),
                "max(0.0, ((" + scale + " - " + valueExpl + ")/" + scale + Functions.nameOrEmptyArg(functionName) + ")"
            );
        }

        @Override
        public double processScale(double scale, double decay) {
            return scale / (1.0 - decay);
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj)) {
                return true;
            }
            return obj != null && getClass() != obj.getClass();
        }
    }
}
