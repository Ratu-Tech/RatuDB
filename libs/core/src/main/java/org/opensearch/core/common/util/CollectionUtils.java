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

package org.opensearch.core.common.util;

import org.opensearch.common.collect.Iterators;
import org.opensearch.core.common.Strings;

import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;

/**
 * Collections-related utility methods.
 *
 * @opensearch.internal
 */
public class CollectionUtils {

    /**
     * Checks if the given array contains any elements.
     *
     * @param array The array to check
     *
     * @return false if the array contains an element, true if not or the array is null.
     */
    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    /**
     * Return a rotated view of the given list with the given distance.
     * <ul>
     * <li>The distance can be negative, in which case the list is rotated to the left.</li>
     * <li>The distance can be larger than the size of the list, in which case the list is rotated multiple times.</li>
     * <li>The distance can be zero, in which case the list is not rotated.</li>
     * <li>The list can be empty, in which case it remains empty.</li>
     * </ul>
     * @param list the list to rotate
     * @param distance the distance to rotate (positive rotates right, negative rotates left)
     * @return a rotated view of the given list with the given distance
     * @see RotatedList
     */
    public static <T> List<T> rotate(final List<T> list, int distance) {
        if (list.isEmpty()) {
            return list;
        }

        int d = distance % list.size();
        if (d < 0) {
            d += list.size();
        }

        if (d == 0) {
            return list;
        }

        return new RotatedList<>(list, d);
    }

    /**
     * In place de-duplicates items in a list
     * Noop if the list is empty or has one item.
     *
     * @throws NullPointerException if the list is `null` or comparator is `null`
     * @param array the list to de-duplicate
     * @param comparator the comparator to use to compare items
     * @param <T> the type of the items in the list
     */
    public static <T> void sortAndDedup(final List<T> array, Comparator<T> comparator) {
        // base case: one item
        if (array.size() <= 1) {
            return;
        }
        array.sort(comparator);
        ListIterator<T> deduped = array.listIterator();
        T cmp = deduped.next(); // return the first item and advance
        Iterator<T> oldArray = array.iterator();
        oldArray.next(); // advance to the old to the second item (advanced to third below)

        do {
            T old = oldArray.next(); // get the next item and advance iter
            if (comparator.compare(cmp, old) != 0 && (cmp = deduped.next()) != old) {
                deduped.set(old);
            }
        } while (oldArray.hasNext());
        // in place update
        array.subList(deduped.nextIndex(), array.size()).clear();
    }

    /**
     * Converts a collection of Integers to an array of ints.
     * @param ints The collection of Integers to convert
     * @return The array of ints
     * @throws NullPointerException if ints is null
     */
    public static int[] toArray(Collection<Integer> ints) {
        Objects.requireNonNull(ints);
        return ints.stream().mapToInt(s -> s).toArray();
    }

    /**
     * Deeply inspects a Map, Iterable, or Object array looking for references back to itself.
     * @throws IllegalArgumentException if a self-reference is found
     * @param value The object to evaluate looking for self references
     * @param messageHint A string to be included in the exception message if the call fails, to provide
     *                    more context to the handler of the exception
     */
    public static void ensureNoSelfReferences(Object value, String messageHint) {
        Iterable<?> it = convert(value);
        if (it != null) {
            ensureNoSelfReferences(it, value, Collections.newSetFromMap(new IdentityHashMap<>()), messageHint);
        }
    }

    /**
     * Converts an object to an Iterable, if possible.
     * @param value The object to convert
     * @return The Iterable, or null if the object cannot be converted
     */
    @SuppressWarnings("unchecked")
    private static Iterable<?> convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return () -> Iterators.concat(map.keySet().iterator(), map.values().iterator());
        } else if ((value instanceof Iterable) && (value instanceof Path == false)) {
            return (Iterable<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(
        final Iterable<?> value,
        Object originalReference,
        final Set<Object> ancestors,
        String messageHint
    ) {
        if (value != null) {
            if (ancestors.add(originalReference) == false) {
                String suffix = Strings.isNullOrEmpty(messageHint) ? "" : String.format(Locale.ROOT, " (%s)", messageHint);
                throw new IllegalArgumentException("Iterable object is self-referencing itself" + suffix);
            }
            for (Object o : value) {
                ensureNoSelfReferences(convert(o), o, ancestors, messageHint);
            }
            ancestors.remove(originalReference);
        }
    }

    /**
     * Returns an unmodifiable copy of the given map.
     * @param map Map to copy
     * @return unmodifiable copy of the map
     */
    public static <R, T> Map<R, T> copyMap(Map<R, T> map) {
        if (map.isEmpty()) {
            return Collections.emptyMap();
        } else {
            return Collections.unmodifiableMap(new HashMap<>(map));
        }
    }

    /**
     * A rotated list
     *
     * @opensearch.internal
     */
    private static class RotatedList<T> extends AbstractList<T> implements RandomAccess {

        private final List<T> in;
        private final int distance;

        /**
         * Creates a rotated list
         * @param list The list to rotate
         * @param distance The distance to rotate to the right
         * @throws IllegalArgumentException if the distance is negative or greater than the size of the list;
         *                                  or if the list is not a {@link RandomAccess} list
         */
        RotatedList(List<T> list, int distance) {
            if (distance < 0 || distance >= list.size()) {
                throw new IllegalArgumentException();
            }
            if (!(list instanceof RandomAccess)) {
                throw new IllegalArgumentException();
            }
            this.in = list;
            this.distance = distance;
        }

        @Override
        public T get(int index) {
            int idx = distance + index;
            if (idx < 0 || idx >= in.size()) {
                idx -= in.size();
            }
            return in.get(idx);
        }

        @Override
        public int size() {
            return in.size();
        }
    }

    /**
     * Converts an {@link Iterable} to an {@link ArrayList}.
     * @param elements The iterable to convert
     * @param <E> the type the elements
     * @return an {@link ArrayList}
     * @throws NullPointerException if elements is null
     */
    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> iterableAsArrayList(Iterable<? extends E> elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        if (elements instanceof Collection) {
            return new ArrayList<>((Collection) elements);
        } else {
            ArrayList<E> list = new ArrayList<>();
            for (E element : elements) {
                list.add(element);
            }
            return list;
        }
    }

    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> arrayAsArrayList(E... elements) {
        if (elements == null) {
            throw new NullPointerException("elements");
        }
        return new ArrayList<>(Arrays.asList(elements));
    }

    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> asArrayList(E first, E... other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        ArrayList<E> list = new ArrayList<>(1 + other.length);
        list.add(first);
        list.addAll(Arrays.asList(other));
        return list;
    }

    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> asArrayList(E first, E second, E... other) {
        if (other == null) {
            throw new NullPointerException("other");
        }
        ArrayList<E> list = new ArrayList<>(1 + 1 + other.length);
        list.add(first);
        list.add(second);
        list.addAll(Arrays.asList(other));
        return list;
    }

    public static <E> ArrayList<E> newSingletonArrayList(E element) {
        return new ArrayList<>(Collections.singletonList(element));
    }

    public static <E> List<List<E>> eagerPartition(List<E> list, int size) {
        if (list == null) {
            throw new NullPointerException("list");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size <= 0");
        }
        List<List<E>> result = new ArrayList<>((int) Math.ceil(list.size() / size));

        List<E> accumulator = new ArrayList<>(size);
        int count = 0;
        for (E element : list) {
            if (count == size) {
                result.add(accumulator);
                accumulator = new ArrayList<>(size);
                count = 0;
            }
            accumulator.add(element);
            count++;
        }
        if (count > 0) {
            result.add(accumulator);
        }

        return result;
    }

    /**
     * Checks if a collection is empty or not. Empty collection mean either it is null or it has no elements in it.
     * If collection contains a null element it means it is not empty.
     *
     * @param collection {@link Collection}
     * @return true if collection is null or {@code isEmpty()}, false otherwise
     * @param <E> Element
     */
    public static <E> boolean isEmpty(final Collection<E> collection) {
        return collection == null || collection.isEmpty();
    }
}
