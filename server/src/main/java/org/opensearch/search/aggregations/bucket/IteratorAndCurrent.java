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

package org.opensearch.search.aggregations.bucket;

import org.opensearch.search.aggregations.InternalMultiBucketAggregation;

import java.util.Iterator;

/**
 * Utility class for keeping track of a current item in an iterator
 *
 * @opensearch.internal
 */
public class IteratorAndCurrent<B extends InternalMultiBucketAggregation.InternalBucket> implements Iterator<B> {
    private final Iterator<B> iterator;
    private B current;

    public IteratorAndCurrent(Iterator<B> iterator) {
        this.iterator = iterator;
        this.current = iterator.next();
    }

    public Iterator<B> getIterator() {
        return iterator;
    }

    public B current() {
        return current;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public B next() {
        return current = iterator.next();
    }
}
