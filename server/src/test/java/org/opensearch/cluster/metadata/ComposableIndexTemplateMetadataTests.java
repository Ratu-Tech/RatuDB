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

package org.opensearch.cluster.metadata;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ComposableIndexTemplateMetadataTests extends AbstractNamedWriteableTestCase<ComposableIndexTemplateMetadata> {
    @Override
    protected ComposableIndexTemplateMetadata createTestInstance() {
        if (randomBoolean()) {
            return new ComposableIndexTemplateMetadata(Collections.emptyMap());
        }
        Map<String, ComposableIndexTemplate> templates = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            templates.put(randomAlphaOfLength(5), ComposableIndexTemplateTests.randomInstance());
        }
        return new ComposableIndexTemplateMetadata(templates);
    }

    @Override
    protected ComposableIndexTemplateMetadata mutateInstance(ComposableIndexTemplateMetadata instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(
                    ComposableIndexTemplateMetadata.class,
                    ComposableIndexTemplateMetadata.TYPE,
                    ComposableIndexTemplateMetadata::new
                )
            )
        );
    }

    @Override
    protected Class<ComposableIndexTemplateMetadata> categoryClass() {
        return ComposableIndexTemplateMetadata.class;
    }
}
