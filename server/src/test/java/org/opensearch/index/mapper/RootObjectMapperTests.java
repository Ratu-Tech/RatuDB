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

package org.opensearch.index.mapper;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;

public class RootObjectMapperTests extends OpenSearchSingleNodeTestCase {

    public void testNumericDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("numeric_detection", false)
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("numeric_detection", true)
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("date_detection", true)
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // update with a different explicit value
        String mapping2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("date_detection", false)
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping2, mapper.mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_date_formats", Arrays.asList("yyyy-MM-dd"))
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), reason);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_date_formats", Arrays.asList())
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), reason);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("dynamic_templates")
            .startObject()
            .startObject("my_template")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping2), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping, mapper.mappingSource().toString());

        String mapping3 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .field("dynamic_templates", Arrays.asList())
            .endObject()
            .endObject()
            .toString();
        mapper = mapperService.merge("type", new CompressedXContent(mapping3), MergeReason.MAPPING_UPDATE);
        assertEquals(mapping3, mapper.mappingSource().toString());
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("dynamic_templates")
            .startObject()
            .startObject("first_template")
            .field("path_match", "first")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("second_template")
            .field("path_match", "second")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper mapper = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping),
            MergeReason.INDEX_TEMPLATE
        );

        DynamicTemplate[] templates = mapper.root().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch());

        // Dynamic templates should be appended and deduplicated.
        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("dynamic_templates")
            .startObject()
            .startObject("third_template")
            .field("path_match", "third")
            .startObject("mapping")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .startObject()
            .startObject("second_template")
            .field("path_match", "second_updated")
            .startObject("mapping")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .toString();
        mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        templates = mapper.root().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch());
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("dynamic_date_formats")
            .startArray()
            .value("test_format")
            .endArray()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startArray("date_formats")
            .startArray()
            .value("test_format")
            .endArray()
            .endArray()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(m))
            );
            assertEquals("Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("dynamic_templates")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> parser.parse("type", new CompressedXContent(mapping)));
        assertEquals("Dynamic template syntax error. An array of named objects is expected.", e.getMessage());
    }

    public void testIllegalDynamicTemplateUnknownFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template1");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings(
            "dynamic template [my_template1] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":"
                + "\"string\"}}], caused by [No mapper found for type [string]]"
        );
    }

    public void testIllegalDynamicTemplateUnknownAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template2");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
        assertWarnings(
            "dynamic template [my_template2] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{"
                + "\"foo\":\"bar\",\"type\":\"keyword\"}}], "
                + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template2] of type [keyword]]"
        );
    }

    public void testIllegalDynamicTemplateInvalidAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("type");
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template3");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "text");
                mapping.field("analyzer", "foobar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
        assertThat(mapper.mappingSource().toString(), containsString("\"analyzer\":\"foobar\""));
        assertWarnings(
            "dynamic template [my_template3] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{"
                + "\"analyzer\":\"foobar\",\"type\":\"text\"}}], caused by [analyzer [foobar] has not been configured in mappings]"
        );
    }

    public void testIllegalDynamicTemplateNoMappingType() throws Exception {
        MapperService mapperService;

        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template4");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("index_phrases", true);
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            mapperService = createIndex("test").mapperService();
            DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"index_phrases\":true"));
        }
        {
            boolean useMatchMappingType = randomBoolean();
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject("type");
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template4");
                    if (useMatchMappingType) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("foo", "bar");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();

            DocumentMapper mapper = mapperService.merge("type", new CompressedXContent(mapping.toString()), MergeReason.MAPPING_UPDATE);
            assertThat(mapper.mappingSource().toString(), containsString("\"foo\":\"bar\""));
            if (useMatchMappingType) {
                assertWarnings(
                    "dynamic template [my_template4] has invalid content [{\"match_mapping_type\":\"*\",\"mapping\":{"
                        + "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], "
                        + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template4] of type [binary]]"
                );
            } else {
                assertWarnings(
                    "dynamic template [my_template4] has invalid content [{\"match\":\"string_*\",\"mapping\":{"
                        + "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], "
                        + "caused by [unknown parameter [foo] on mapper [__dynamic__my_template4] of type [binary]]"
                );
            }
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }
}
