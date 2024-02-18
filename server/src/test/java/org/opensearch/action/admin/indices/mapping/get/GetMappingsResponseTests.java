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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GetMappingsResponseTests extends AbstractWireSerializingTestCase<GetMappingsResponse> {

    public void testCheckEqualsAndHashCode() {
        GetMappingsResponse resp = createTestInstance();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(resp, r -> new GetMappingsResponse(r.mappings()), GetMappingsResponseTests::mutate);
    }

    @Override
    protected Writeable.Reader<GetMappingsResponse> instanceReader() {
        return GetMappingsResponse::new;
    }

    private static GetMappingsResponse mutate(GetMappingsResponse original) {
        final Map<String, MappingMetadata> builder = new HashMap<>(original.mappings());
        String indexKey = original.mappings().keySet().iterator().next();
        builder.put(indexKey + "1", createMappingsForIndex());

        return new GetMappingsResponse(builder);
    }

    @Override
    protected GetMappingsResponse mutateInstance(GetMappingsResponse instance) throws IOException {
        return mutate(instance);
    }

    public static MappingMetadata createMappingsForIndex() {
        Map<String, Object> mappings = new HashMap<>();
        if (rarely() == false) { // rarely have no fields
            mappings.put("field", randomFieldMapping());
            if (randomBoolean()) {
                mappings.put("field2", randomFieldMapping());
            }
            String typeName = MapperService.SINGLE_MAPPING_NAME;
            return new MappingMetadata(typeName, mappings);
        }
        return new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, mappings);
    }

    @Override
    protected GetMappingsResponse createTestInstance() {
        final Map<String, MappingMetadata> indexBuilder = new HashMap<>();
        indexBuilder.put("index-" + randomAlphaOfLength(5), createMappingsForIndex());
        GetMappingsResponse resp = new GetMappingsResponse(indexBuilder);
        logger.debug("--> created: {}", resp);
        return resp;
    }

    // Not meant to be exhaustive
    private static Map<String, Object> randomFieldMapping() {
        Map<String, Object> mappings = new HashMap<>();
        if (randomBoolean()) {
            mappings.put("type", randomBoolean() ? "text" : "keyword");
            mappings.put("index", "analyzed");
            mappings.put("analyzer", "english");
        } else if (randomBoolean()) {
            mappings.put("type", randomFrom("integer", "float", "long", "double"));
            mappings.put("index", Objects.toString(randomBoolean()));
        } else if (randomBoolean()) {
            mappings.put("type", "object");
            mappings.put("dynamic", "strict");
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> props1 = new HashMap<>();
            props1.put("type", randomFrom("text", "keyword"));
            props1.put("analyzer", "keyword");
            properties.put("subtext", props1);
            Map<String, Object> props2 = new HashMap<>();
            props2.put("type", "object");
            Map<String, Object> prop2properties = new HashMap<>();
            Map<String, Object> props3 = new HashMap<>();
            props3.put("type", "integer");
            props3.put("index", "false");
            prop2properties.put("subsubfield", props3);
            props2.put("properties", prop2properties);
            mappings.put("properties", properties);
        } else {
            mappings.put("type", "keyword");
        }
        return mappings;
    }
}
