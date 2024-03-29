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

package org.opensearch.gradle;

import org.gradle.api.Named;
import org.gradle.api.tasks.Input;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LazyPropertyMap<K, V> extends AbstractLazyPropertyCollection implements Map<K, V> {

    private final Map<K, PropertyMapEntry<K, V>> delegate = new LinkedHashMap<>();
    private final BiFunction<K, V, ?> normalizationMapper;

    public LazyPropertyMap(String name) {
        this(name, null);
    }

    public LazyPropertyMap(String name, Object owner) {
        this(name, owner, null);
    }

    public LazyPropertyMap(String name, Object owner, BiFunction<K, V, ?> normalizationMapper) {
        super(name, owner);
        this.normalizationMapper = normalizationMapper;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.values().stream().map(PropertyMapEntry::getValue).anyMatch(v -> v.equals(value));
    }

    @Override
    public V get(Object key) {
        PropertyMapEntry<K, V> entry = delegate.get(key);
        if (entry != null) {
            V value = entry.getValue();
            assertNotNull(value, "value for key '" + key + "'");
            return value;
        } else {
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, PropertyNormalization.DEFAULT);
    }

    public V put(K key, V value, PropertyNormalization normalization) {
        assertNotNull(value, "value for key '" + key + "'");
        return put(key, () -> value, normalization);
    }

    public V put(K key, Supplier<V> supplier) {
        return put(key, supplier, PropertyNormalization.DEFAULT);
    }

    public V put(K key, Supplier<V> supplier, PropertyNormalization normalization) {
        assertNotNull(supplier, "supplier for key '" + key + "'");
        PropertyMapEntry<K, V> previous = delegate.put(key, new PropertyMapEntry<>(key, supplier, normalization));
        return previous == null ? null : previous.getValue();
    }

    @Override
    public V remove(Object key) {
        PropertyMapEntry<K, V> previous = delegate.remove(key);
        return previous == null ? null : previous.getValue();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support putAll()");
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set<K> keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection<V> values() {
        return delegate.values().stream().peek(this::validate).map(PropertyMapEntry::getValue).collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return delegate.entrySet()
            .stream()
            .peek(this::validate)
            .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getValue()))
            .entrySet();
    }

    @Override
    public List<? extends Object> getNormalizedCollection() {
        return delegate.values()
            .stream()
            .peek(this::validate)
            .filter(entry -> entry.getNormalization() != PropertyNormalization.IGNORE_VALUE)
            .map(entry -> normalizationMapper == null ? entry : normalizationMapper.apply(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    }

    private void validate(Entry<K, PropertyMapEntry<K, V>> entry) {
        validate(entry.getValue());
    }

    private void validate(PropertyMapEntry<K, V> supplier) {
        assertNotNull(supplier, "key '" + supplier.getKey() + "' supplier value");
    }

    private static class PropertyMapEntry<K, V> implements Named {
        private final K key;
        private final Supplier<V> value;
        private final PropertyNormalization normalization;

        PropertyMapEntry(K key, Supplier<V> value, PropertyNormalization normalization) {
            this.key = key;
            this.value = value;
            this.normalization = normalization;
        }

        @Input
        public PropertyNormalization getNormalization() {
            return normalization;
        }

        @Override
        @Input
        public String getName() {
            return getKey().toString();
        }

        @Input
        public K getKey() {
            return key;
        }

        @Input
        public V getValue() {
            return value.get();
        }
    }
}
