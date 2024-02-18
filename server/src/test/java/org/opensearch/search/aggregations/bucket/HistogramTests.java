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

package org.opensearch.search.aggregations.bucket;

import org.opensearch.search.aggregations.BaseAggregationTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class HistogramTests extends BaseAggregationTestCase<HistogramAggregationBuilder> {

    @Override
    protected HistogramAggregationBuilder createTestAggregatorBuilder() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);
        if (randomBoolean()) {
            double minBound = randomDouble();
            double maxBound = randomDoubleBetween(minBound, 1, true);
            factory.extendedBounds(minBound, maxBound);
        }
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.minDocCount(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            factory.missing(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            factory.offset(randomIntBetween(0, 100000));
        }
        if (randomBoolean()) {
            List<BucketOrder> order = randomOrder();
            if (order.size() == 1 && randomBoolean()) {
                factory.order(order.get(0));
            } else {
                factory.order(order);
            }
        }
        return factory;
    }

    public void testInvalidBounds() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");
        factory.field(INT_FIELD_NAME);
        factory.interval(randomDouble() * 1000);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NaN, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.POSITIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(Double.NEGATIVE_INFINITY, 1.0); });
        assertThat(ex.getMessage(), startsWith("min bound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NaN); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.POSITIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));
        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.0, Double.NEGATIVE_INFINITY); });
        assertThat(ex.getMessage(), startsWith("max bound must be finite, got: "));

        ex = expectThrows(IllegalArgumentException.class, () -> { factory.extendedBounds(0.5, 0.4); });
        assertThat(ex.getMessage(), equalTo("max bound [0.4] must be greater than min bound [0.5]"));
    }

    /**
     * Check that minBound/maxBound does not throw {@link NullPointerException} when called before being set.
     */
    public void testMinBoundMaxBoundDefaultValues() {
        HistogramAggregationBuilder factory = new HistogramAggregationBuilder("foo");

        double minValue = factory.minBound();
        double maxValue = factory.maxBound();

        assertThat(minValue, equalTo(Double.POSITIVE_INFINITY));
        assertThat(maxValue, equalTo(Double.NEGATIVE_INFINITY));
    }

    private List<BucketOrder> randomOrder() {
        List<BucketOrder> orders = new ArrayList<>();
        switch (randomInt(4)) {
            case 0:
                orders.add(BucketOrder.key(randomBoolean()));
                break;
            case 1:
                orders.add(BucketOrder.count(randomBoolean()));
                break;
            case 2:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 3:
                orders.add(BucketOrder.aggregation(randomAlphaOfLengthBetween(3, 20), randomAlphaOfLengthBetween(3, 20), randomBoolean()));
                break;
            case 4:
                int numOrders = randomIntBetween(1, 3);
                for (int i = 0; i < numOrders; i++) {
                    orders.addAll(randomOrder());
                }
                break;
            default:
                fail();
        }
        return orders;
    }

}
