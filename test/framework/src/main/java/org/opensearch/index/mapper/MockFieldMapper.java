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

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.List;

// this sucks how much must be overridden just do get a dummy field mapper...
public class MockFieldMapper extends ParametrizedFieldMapper {
    static Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();

    public MockFieldMapper(String fullName) {
        this(new FakeFieldType(fullName));
    }

    public MockFieldMapper(MappedFieldType fieldType) {
        super(findSimpleName(fieldType.name()), fieldType, MultiFields.empty(), new CopyTo.Builder().build());
    }

    public MockFieldMapper(String fullName, MappedFieldType fieldType, MultiFields multifields, CopyTo copyTo) {
        super(findSimpleName(fullName), fieldType, multifields, copyTo);
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName());
    }

    static String findSimpleName(String fullName) {
        int ndx = fullName.lastIndexOf('.');
        return fullName.substring(ndx + 1);
    }

    public static class FakeFieldType extends TermBasedFieldType {
        public FakeFieldType(String name) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return "faketype";
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    protected String contentType() {
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context) {}

    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final MappedFieldType fieldType;

        protected Builder(String name) {
            super(name);
            this.fieldType = new FakeFieldType(name);
            this.builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.emptyList();
        }

        public Builder addMultiField(Builder builder) {
            this.multiFieldsBuilder.add(builder);
            return this;
        }

        public Builder copyTo(String field) {
            this.copyTo.add(field);
            return this;
        }

        @Override
        public MockFieldMapper build(BuilderContext context) {
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            return new MockFieldMapper(name(), fieldType, multiFields, copyTo.build());
        }
    }
}
