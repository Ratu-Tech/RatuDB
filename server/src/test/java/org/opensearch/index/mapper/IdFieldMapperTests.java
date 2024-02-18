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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.index.mapper.IdFieldMapper.ID_FIELD_DATA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;

public class IdFieldMapperTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().toString();
        DocumentMapper docMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("_id", "1").endObject()),
                    MediaTypeRegistry.JSON
                )
            );
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(
                e.getCause().getMessage(),
                e.getCause().getMessage().contains("Field [_id] is a metadata field and cannot be added inside a document")
            );
        }
    }

    public void testDefaults() throws IOException {
        Settings indexSettings = Settings.EMPTY;
        MapperService mapperService = createIndex("test", indexSettings).mapperService();
        DocumentMapper mapper = mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        ParsedDocument document = mapper.parse(new SourceToParse("index", "id", new BytesArray("{}"), MediaTypeRegistry.JSON));
        IndexableField[] fields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.DOCS, fields[0].fieldType().indexOptions());
        assertTrue(fields[0].fieldType().stored());
        assertEquals(Uid.encodeId("id"), fields[0].binaryValue());
    }

    public void testEnableFieldData() throws IOException {
        IndexService service = createIndex("test", Settings.EMPTY);
        MapperService mapperService = service.mapperService();
        mapperService.merge("type", new CompressedXContent("{\"type\":{}}"), MergeReason.MAPPING_UPDATE);
        IdFieldMapper.IdFieldType ft = (IdFieldMapper.IdFieldType) service.mapperService().fieldType("_id");

        ft.fielddataBuilder("test", () -> { throw new UnsupportedOperationException(); }).build(null, null);
        assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        assertTrue(ft.isAggregatable());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey(), false))
            .get();
        try {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> ft.fielddataBuilder("test", () -> {
                throw new UnsupportedOperationException();
            }).build(null, null));
            assertThat(exc.getMessage(), containsString(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
            assertFalse(ft.isAggregatable());
        } finally {
            // unset cluster setting
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()))
                .get();
        }
    }
}
