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

package org.opensearch.painless;

public class InjectionTests extends ScriptTestCase {

    public void testInjection() {
        assertEquals(16, exec("org.opensearch.painless.FeatureTestObject.staticNumberArgument(8);"));
    }

    public void testInstanceInjection() {
        assertEquals(
            1000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.injectTimesX(5)"
            )
        );
    }

    public void testInstanceInjectWithLambda() {
        assertEquals(
            2000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.injectWithLambda(x -> 2*x, 5)"
            )
        );
    }

    public void testInstanceInjectWithDefLambda() {
        assertEquals(2000, exec("def f = new org.opensearch.painless.FeatureTestObject(100, 0); f.injectWithLambda(x -> 2*x, (short)5)"));
    }

    public void testInjectionOnDefNoInject() {
        assertEquals(1000, exec("def d = new org.opensearch.painless.FeatureTestObject(100, 0); d.injectTimesX((short)5)"));
    }

    public void testInjectionOnMethodReference() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "org.opensearch.painless.FeatureTestObject ft1 = "
                    + "       new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testInjectionOnMethodReference2() {
        assertEquals(
            60,
            exec(
                "org.opensearch.painless.FeatureTestObject ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testInjectionOnMethodReference3() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedInstanceInjection() {
        assertEquals(
            1000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.augmentInjectTimesX(5)"
            )
        );
    }

    public void testAugmentedInstanceInjectWithLambda() {
        assertEquals(
            2000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.augmentInjectWithLambda(x -> 2*x, 5)"
            )
        );
    }

    public void testAugmentedInstanceInjectWithDefLambda() {
        assertEquals(
            2000,
            exec("def f = new org.opensearch.painless.FeatureTestObject(100, 0); f.augmentInjectWithLambda(x -> 2*x, (short)5)")
        );
    }

    public void testAugmentedInjectionOnDefNoInject() {
        assertEquals(1000, exec("def d = new org.opensearch.painless.FeatureTestObject(100, 0); d.augmentInjectTimesX((short)5)"));
    }

    public void testAugmentedInjectionOnMethodReference() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "org.opensearch.painless.FeatureTestObject ft1 = "
                    + "       new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedInjectionOnMethodReference2() {
        assertEquals(
            60,
            exec(
                "org.opensearch.painless.FeatureTestObject ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedInjectionOnMethodReference3() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectTimesX, (short)3, 5)"
            )
        );
    }

    public void testInstanceMultiInjection() {
        assertEquals(
            6000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.injectMultiTimesX(5)"
            )
        );
    }

    public void testInstanceMultiInjectWithLambda() {
        assertEquals(
            8000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.injectMultiWithLambda(x -> 2*x, 5)"
            )
        );
    }

    public void testInstanceMultiInjectWithDefLambda() {
        assertEquals(2000, exec("def f = new org.opensearch.painless.FeatureTestObject(100, 0); f.injectWithLambda(x -> 2*x, (short)5)"));
    }

    public void testMultiInjectionOnDefNoMultiInject() {
        assertEquals(6000, exec("def d = new org.opensearch.painless.FeatureTestObject(100, 0); d.injectMultiTimesX((short)5)"));
    }

    public void testMultiInjectionOnMethodReference() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "org.opensearch.painless.FeatureTestObject ft1 = "
                    + "       new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testMultiInjectionOnMethodReference2() {
        assertEquals(
            60,
            exec(
                "org.opensearch.painless.FeatureTestObject ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testMultiInjectionOnMethodReference3() {
        assertEquals(
            60,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.timesSupplier(ft0::injectTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedInstanceMultiInjection() {
        assertEquals(
            5000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.augmentInjectMultiTimesX(5)"
            )
        );
    }

    public void testAugmentedInstanceMultiInjectWithLambda() {
        assertEquals(
            20000,
            exec(
                "org.opensearch.painless.FeatureTestObject f = new org.opensearch.painless.FeatureTestObject(100, 0); "
                    + "f.augmentInjectMultiWithLambda(x -> 2*x, 5)"
            )
        );
    }

    public void testAugmentedInstanceMultiInjectWithDefLambda() {
        assertEquals(
            20000,
            exec("def f = new org.opensearch.painless.FeatureTestObject(100, 0); " + "f.augmentInjectMultiWithLambda(x -> 2*x, (short)5)")
        );
    }

    public void testAugmentedMultiInjectionOnDefNoMultiInject() {
        assertEquals(5000, exec("def d = new org.opensearch.painless.FeatureTestObject(100, 0); d.augmentInjectMultiTimesX((short)5)"));
    }

    public void testAugmentedMultiInjectionOnMethodReference() {
        assertEquals(
            300,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "org.opensearch.painless.FeatureTestObject ft1 = "
                    + "       new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectMultiTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedMultiInjectionOnMethodReference2() {
        assertEquals(
            300,
            exec(
                "org.opensearch.painless.FeatureTestObject ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectMultiTimesX, (short)3, 5)"
            )
        );
    }

    public void testAugmentedMultiInjectionOnMethodReference3() {
        assertEquals(
            300,
            exec(
                "def ft0 = new org.opensearch.painless.FeatureTestObject(2, 0); "
                    + "def ft1 = new org.opensearch.painless.FeatureTestObject(1000, 0); "
                    + "ft1.augmentTimesSupplier(ft0::augmentInjectMultiTimesX, (short)3, 5)"
            )
        );
    }
}
