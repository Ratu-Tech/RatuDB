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

package org.opensearch.join.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.StringFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Simple field mapper hack to ensure that there is a one and only {@link ParentJoinFieldMapper} per mapping.
 * This field mapper is not used to index or query any data, it is used as a marker in the mapping that
 * denotes the presence of a parent-join field and forbids the addition of any additional ones.
 * This class is also used to quickly retrieve the parent-join field defined in a mapping without
 * specifying the name of the field.
 */
public class MetaJoinFieldMapper extends FieldMapper {
    static final String NAME = "_parent_join";
    static final String CONTENT_TYPE = "parent_join";

    static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.freeze();
        }
    }

    static class Builder extends FieldMapper.Builder<Builder> {

        final String joinField;

        Builder(String joinField) {
            super(NAME, Defaults.FIELD_TYPE);
            builder = this;
            this.joinField = joinField;
        }

        @Override
        public MetaJoinFieldMapper build(BuilderContext context) {
            return new MetaJoinFieldMapper(name, joinField);
        }
    }

    public static class MetaJoinFieldType extends StringFieldType {

        private final String joinField;

        private MetaJoinFieldType(String joinField) {
            super(NAME, false, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.joinField = joinField;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException("Cannot fetch values for metadata field [" + typeName() + "].");
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        public String getJoinField() {
            return joinField;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException("Exists query not supported for fields of type" + typeName());
        }
    }

    MetaJoinFieldMapper(String name, String joinField) {
        super(name, Defaults.FIELD_TYPE, new MetaJoinFieldType(joinField), MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public MetaJoinFieldType fieldType() {
        return (MetaJoinFieldType) super.fieldType();
    }

    @Override
    protected MetaJoinFieldMapper clone() {
        return (MetaJoinFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {}

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new IllegalStateException("Should never be called");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
