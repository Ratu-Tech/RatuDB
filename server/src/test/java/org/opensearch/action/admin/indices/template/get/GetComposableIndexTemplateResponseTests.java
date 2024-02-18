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

package org.opensearch.action.admin.indices.template.get;

import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.ComposableIndexTemplateTests;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetComposableIndexTemplateResponseTests extends AbstractWireSerializingTestCase<GetComposableIndexTemplateAction.Response> {
    @Override
    protected Writeable.Reader<GetComposableIndexTemplateAction.Response> instanceReader() {
        return GetComposableIndexTemplateAction.Response::new;
    }

    @Override
    protected GetComposableIndexTemplateAction.Response createTestInstance() {
        if (randomBoolean()) {
            return new GetComposableIndexTemplateAction.Response(Collections.emptyMap());
        }
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 4); i++) {
            templates.put(randomAlphaOfLength(4), ComposableIndexTemplateTests.randomInstance());
        }
        return new GetComposableIndexTemplateAction.Response(templates);
    }

    @Override
    protected GetComposableIndexTemplateAction.Response mutateInstance(GetComposableIndexTemplateAction.Response instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
