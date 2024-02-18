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
package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;

import static org.hamcrest.Matchers.containsString;

public class DocCountFieldMapperTests extends MapperServiceTestCase {

    private static final String CONTENT_TYPE = DocCountFieldMapper.CONTENT_TYPE;
    private static final String DOC_COUNT_FIELD = DocCountFieldMapper.NAME;

    /**
     * Test parsing field mapping and adding simple field
     */
    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", 500).field(CONTENT_TYPE, 100)));

        IndexableField field = doc.rootDoc().getField(DOC_COUNT_FIELD);
        assertEquals(100L, field.numericValue());
        assertEquals(DocValuesType.NUMERIC, field.fieldType().docValuesType());
        assertEquals(1, doc.rootDoc().getFields(DOC_COUNT_FIELD).length);
    }

    public void testInvalidDocument_NegativeDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, -100))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_ZeroDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 0))));
        assertThat(e.getCause().getMessage(), containsString("Field [_doc_count] must be a positive integer"));
    }

    public void testInvalidDocument_NonNumericDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, "foo"))));
        assertThat(
            e.getCause().getMessage(),
            containsString("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testInvalidDocument_FractionalDocCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> b.field(CONTENT_TYPE, 100.23))));
        assertThat(e.getCause().getMessage(), containsString("100.23 cannot be converted to Long without data loss"));
    }
}
