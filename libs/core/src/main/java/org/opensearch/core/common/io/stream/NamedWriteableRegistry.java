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

package org.opensearch.core.common.io.stream;

import org.opensearch.common.annotation.PublicApi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A registry for {@link Writeable.Reader} readers of {@link NamedWriteable}.
 * <p>
 * The registration is keyed by the combination of the category class of {@link NamedWriteable}, and a name unique
 * to that category.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class NamedWriteableRegistry {

    /**
     * An entry in the registry, made up of a category class and name, and a reader for that category class.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Entry {

        /** The superclass of a {@link NamedWriteable} which will be read by {@link #reader}. */
        public final Class<?> categoryClass;

        /** A name for the writeable which is unique to the {@link #categoryClass}. */
        public final String name;

        /** A reader capability of reading*/
        public final Writeable.Reader<?> reader;

        /** Creates a new entry which can be stored by the registry. */
        public <T extends NamedWriteable> Entry(Class<T> categoryClass, String name, Writeable.Reader<? extends T> reader) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.reader = Objects.requireNonNull(reader);
        }
    }

    /**
     * The underlying data of the registry maps from the category to an inner
     * map of name unique to that category, to the actual reader.
     */
    private final Map<Class<?>, Map<String, Writeable.Reader<?>>> registry;

    /**
     * Constructs a new registry from the given entries.
     */
    public NamedWriteableRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = Collections.emptyMap();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort((e1, e2) -> e1.categoryClass.getName().compareTo(e2.categoryClass.getName()));

        Map<Class<?>, Map<String, Writeable.Reader<?>>> registry = new HashMap<>();
        Map<String, Writeable.Reader<?>> readers = null;
        @SuppressWarnings("rawtypes")
        Class currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, Collections.unmodifiableMap(readers));
                }
                readers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            Writeable.Reader<?> oldReader = readers.put(entry.name, entry.reader);
            if (oldReader != null) {
                throw new IllegalArgumentException(
                    "NamedWriteable ["
                        + currentCategory.getName()
                        + "]["
                        + entry.name
                        + "]"
                        + " is already registered for ["
                        + oldReader.getClass().getName()
                        + "],"
                        + " cannot register ["
                        + entry.reader.getClass().getName()
                        + "]"
                );
            }
        }
        // handle the last category
        registry.put(currentCategory, Collections.unmodifiableMap(readers));

        this.registry = Collections.unmodifiableMap(registry);
    }

    /**
     * Returns a reader for a {@link NamedWriteable} object identified by the
     * name provided as argument and its category.
     */
    public <T> Writeable.Reader<? extends T> getReader(Class<T> categoryClass, String name) {
        Map<String, Writeable.Reader<?>> readers = registry.get(categoryClass);
        if (readers == null) {
            throw new IllegalArgumentException("Unknown NamedWriteable category [" + categoryClass.getName() + "]");
        }
        @SuppressWarnings("unchecked")
        Writeable.Reader<? extends T> reader = (Writeable.Reader<? extends T>) readers.get(name);
        if (reader == null) {
            throw new IllegalArgumentException("Unknown NamedWriteable [" + categoryClass.getName() + "][" + name + "]");
        }
        return reader;
    }
}
