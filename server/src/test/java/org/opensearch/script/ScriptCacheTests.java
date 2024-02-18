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

package org.opensearch.script;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ScriptCacheTests extends OpenSearchTestCase {
    public void testCompileStatusOnLimitExceeded() {
        final TimeValue expire = ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(Settings.EMPTY);
        final Integer size = ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey();
        ScriptCache cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(0, TimeValue.timeValueMinutes(1)), settingName);
        ScriptContext<?> context = randomFrom(ScriptModule.CORE_CONTEXTS.values());
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put("1+1", p -> null); // only care about compilation, not execution
        ScriptEngine engine = new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, scripts, Collections.emptyMap());
        GeneralScriptException ex = expectThrows(
            GeneralScriptException.class,
            () -> cache.compile(context, engine, "1+1", "1+1", ScriptType.INLINE, Collections.emptyMap())
        );
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ex.status());
    }

    // even though circuit breaking is allowed to be configured per minute, we actually weigh this over five minutes
    // simply by multiplying by five, so even setting it to one, requires five compilations to break
    public void testCompilationCircuitBreaking() throws Exception {
        final TimeValue expire = ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(Settings.EMPTY);
        final Integer size = ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey();
        ScriptCache cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(1, TimeValue.timeValueMinutes(1)), settingName);
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(2, TimeValue.timeValueMinutes(1)), settingName);
        cache.checkCompilationLimit(); // should pass
        cache.checkCompilationLimit(); // should pass
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        int count = randomIntBetween(5, 50);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(count, TimeValue.timeValueMinutes(1)), settingName);
        for (int i = 0; i < count; i++) {
            cache.checkCompilationLimit(); // should pass
        }
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(size, expire, new ScriptCache.CompilationRate(0, TimeValue.timeValueMinutes(1)), settingName);
        expectThrows(CircuitBreakingException.class, cache::checkCompilationLimit);
        cache = new ScriptCache(
            size,
            expire,
            new ScriptCache.CompilationRate(Integer.MAX_VALUE, TimeValue.timeValueMinutes(1)),
            settingName
        );
        int largeLimit = randomIntBetween(1000, 10000);
        for (int i = 0; i < largeLimit; i++) {
            cache.checkCompilationLimit();
        }
    }

    public void testUnlimitedCompilationRate() {
        final Integer size = ScriptService.SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(Settings.EMPTY);
        final TimeValue expire = ScriptService.SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(Settings.EMPTY);
        String settingName = ScriptService.SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey();
        ScriptCache cache = new ScriptCache(size, expire, ScriptCache.UNLIMITED_COMPILATION_RATE, settingName);
        ScriptCache.TokenBucketState initialState = cache.tokenBucketState.get();
        for (int i = 0; i < 3000; i++) {
            cache.checkCompilationLimit();
            ScriptCache.TokenBucketState currentState = cache.tokenBucketState.get();
            assertEquals(initialState.lastInlineCompileTime, currentState.lastInlineCompileTime);
            assertEquals(initialState.availableTokens, currentState.availableTokens, 0.0); // delta of 0.0 because it should never change
        }
    }
}
