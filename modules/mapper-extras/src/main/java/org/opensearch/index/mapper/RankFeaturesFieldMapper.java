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

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} that exposes Lucene's {@link FeatureField} as a sparse
 * vector of features.
 */
public class RankFeaturesFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "rank_features";

    private static RankFeaturesFieldType ft(FieldMapper in) {
        return ((RankFeaturesFieldMapper) in).fieldType();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> positiveScoreImpact = Parameter.boolParam(
            "positive_score_impact",
            false,
            m -> ft(m).positiveScoreImpact,
            true
        );
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
            builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, positiveScoreImpact);
        }

        @Override
        public RankFeaturesFieldMapper build(BuilderContext context) {
            return new RankFeaturesFieldMapper(
                name,
                new RankFeaturesFieldType(buildFullName(context), meta.getValue(), positiveScoreImpact.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                positiveScoreImpact.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class RankFeaturesFieldType extends MappedFieldType {

        private final boolean positiveScoreImpact;

        public RankFeaturesFieldType(String name, Map<String, String> meta, boolean positiveScoreImpact) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.positiveScoreImpact = positiveScoreImpact;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public boolean positiveScoreImpact() {
            return positiveScoreImpact;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("[rank_features] fields do not support [exists] queries");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("[rank_features] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [rank_features] fields are not supported");
        }
    }

    private final boolean positiveScoreImpact;

    private RankFeaturesFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        boolean positiveScoreImpact
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.positiveScoreImpact = positiveScoreImpact;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected RankFeaturesFieldMapper clone() {
        return (RankFeaturesFieldMapper) super.clone();
    }

    @Override
    public RankFeaturesFieldType fieldType() {
        return (RankFeaturesFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[rank_features] fields can't be used in multi-fields");
        }

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[rank_features] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        String feature = null;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                feature = context.parser().currentName();
            } else if (token == Token.VALUE_NULL) {
                // ignore feature, this is consistent with numeric fields
            } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                final String key = name() + "." + feature;
                float value = context.parser().floatValue(true);
                if (context.doc().getByKey(key) != null) {
                    throw new IllegalArgumentException(
                        "[rank_features] fields do not support indexing multiple values for the same "
                            + "rank feature ["
                            + key
                            + "] in the same document"
                    );
                }
                if (positiveScoreImpact == false) {
                    value = 1 / value;
                }
                context.doc().addWithKey(key, new FeatureField(name(), feature, value));
            } else {
                throw new IllegalArgumentException(
                    "[rank_features] fields take hashes that map a feature to a strictly positive "
                        + "float, but got unexpected token "
                        + token
                );
            }
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
