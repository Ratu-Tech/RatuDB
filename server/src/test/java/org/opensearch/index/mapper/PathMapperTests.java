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
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

import static org.opensearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PathMapperTests extends OpenSearchSingleNodeTestCase {
    public void testPathMapping() throws IOException {
        String mapping = copyToStringFromClasspath("/org/opensearch/index/mapper/path/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService()
            .documentMapperParser()
            .parse(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping));

        // test full name
        assertThat(docMapper.mappers().getMapper("first1"), nullValue());
        assertThat(docMapper.mappers().getMapper("name1.first1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("last1"), nullValue());
        assertThat(docMapper.mappers().getMapper("i_last_1"), nullValue());
        assertThat(docMapper.mappers().getMapper("name1.last1"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name1.i_last_1"), nullValue());

        assertThat(docMapper.mappers().getMapper("first2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.first2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("last2"), nullValue());
        assertThat(docMapper.mappers().getMapper("i_last_2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.i_last_2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name2.last2"), notNullValue());
    }
}
