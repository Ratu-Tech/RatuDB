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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

import java.io.IOException;
import java.util.Collections;

public class BooleanFieldTypeTests extends FieldTypeTestCase {

    public void testValueFormat() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(false, ft.docValueFormat(null, null).format(0));
        assertEquals(true, ft.docValueFormat(null, null).format(1));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(true, ft.valueForDisplay("T"));
        assertEquals(false, ft.valueForDisplay("F"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay(0));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("true"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("G"));
    }

    public void testTermQuery() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(new TermQuery(new Term("field", "T")), ft.termQuery("true", null));
        assertEquals(new TermQuery(new Term("field", "F")), ft.termQuery("false", null));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("true", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {

        MappedFieldType fieldType = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(Collections.singletonList(true), fetchSourceValue(fieldType, true));
        assertEquals(Collections.singletonList(false), fetchSourceValue(fieldType, "false"));
        assertEquals(Collections.singletonList(false), fetchSourceValue(fieldType, ""));

        MappedFieldType nullFieldType = new BooleanFieldMapper.BooleanFieldType("field", true, false, true, true, Collections.emptyMap());
        assertEquals(Collections.singletonList(true), fetchSourceValue(nullFieldType, null));
    }
}
