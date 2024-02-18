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

import org.opensearch.common.Explicit;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * A mapper for a builtin field containing metadata about a document.
 *
 * @opensearch.internal
 */
public abstract class MetadataFieldMapper extends ParametrizedFieldMapper {

    /**
     * Type parser for the field mapper
     *
     * @opensearch.internal
     */
    public interface TypeParser extends Mapper.TypeParser {

        @Override
        MetadataFieldMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;

        /**
         * Get the default {@link MetadataFieldMapper} to use, if nothing had to be parsed.
         * @param fieldType      the existing field type for this meta mapper on the current index
         *                       or null if this is the first type being introduced
         * @param parserContext context that may be useful to build the field like analyzers
         */
        // TODO: remove the fieldType parameter which is only used for bw compat with pre-2.0
        // since settings could be modified
        MetadataFieldMapper getDefault(MappedFieldType fieldType, ParserContext parserContext);
    }

    /**
     * Declares an updateable boolean parameter for a metadata field
     * <p>
     * We need to distinguish between explicit configuration and default value for metadata
     * fields, because mapping updates will carry over the previous metadata values if a
     * metadata field is not explicitly declared in the update.  A standard boolean
     * parameter explicitly configured with a default value will not be serialized (as
     * we do not serialize default parameters for mapping updates), and as such will be
     * ignored by the update merge.  Instead, we use an {@link Explicit} object that
     * will serialize its value if it has been configured, no matter what the value is.
     */
    public static Parameter<Explicit<Boolean>> updateableBoolParam(
        String name,
        Function<FieldMapper, Explicit<Boolean>> initializer,
        boolean defaultValue
    ) {
        Explicit<Boolean> defaultExplicit = new Explicit<>(defaultValue, false);
        return new Parameter<>(
            name,
            true,
            () -> defaultExplicit,
            (n, c, o) -> new Explicit<>(XContentMapValues.nodeBooleanValue(o), true),
            initializer
        ).setSerializer((b, n, v) -> b.field(n, v.value()), v -> Boolean.toString(v.value()));
    }

    /**
     * A type parser for an unconfigurable metadata field.
     *
     * @opensearch.internal
     */
    public static class FixedTypeParser implements TypeParser {

        final Function<ParserContext, MetadataFieldMapper> mapperParser;

        public FixedTypeParser(Function<ParserContext, MetadataFieldMapper> mapperParser) {
            this.mapperParser = mapperParser;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            throw new MapperParsingException(name + " is not configurable");
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType defaultFieldType, ParserContext parserContext) {
            return mapperParser.apply(parserContext);
        }
    }

    /**
     * Type parser that is configurable
     *
     * @opensearch.internal
     */
    public static class ConfigurableTypeParser implements TypeParser {

        final Function<ParserContext, MetadataFieldMapper> defaultMapperParser;
        final Function<ParserContext, Builder> builderFunction;

        public ConfigurableTypeParser(
            Function<ParserContext, MetadataFieldMapper> defaultMapperParser,
            Function<ParserContext, Builder> builderFunction
        ) {
            this.defaultMapperParser = defaultMapperParser;
            this.builderFunction = builderFunction;
        }

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(parserContext);
            builder.parse(name, parserContext, node);
            return builder;
        }

        @Override
        public MetadataFieldMapper getDefault(MappedFieldType defaultFieldType, ParserContext parserContext) {
            return defaultMapperParser.apply(parserContext);
        }
    }

    /**
     * Base builder for internal metadata fields
     *
     * @opensearch.internal
     */
    public abstract static class Builder extends ParametrizedFieldMapper.Builder {

        protected Builder(String name) {
            super(name);
        }

        boolean isConfigured() {
            for (Parameter<?> param : getParameters()) {
                if (param.isConfigured()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public abstract MetadataFieldMapper build(BuilderContext context);
    }

    protected MetadataFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType.name(), mappedFieldType, MultiFields.empty(), CopyTo.empty());
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return null;    // by default, things can't be configured so we have no builder
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        MetadataFieldMapper.Builder mergeBuilder = (MetadataFieldMapper.Builder) getMergeBuilder();
        if (mergeBuilder == null || mergeBuilder.isConfigured() == false) {
            return builder;
        }
        builder.startObject(simpleName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        getMergeBuilder().toXContent(builder, includeDefaults);
        return builder.endObject();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new MapperParsingException(
            "Field [" + name() + "] is a metadata field and cannot be added inside" + " a document. Use the index API request parameters."
        );
    }

    /**
     * Called before {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public void preParse(ParseContext context) throws IOException {
        // do nothing
    }

    /**
     * Called after {@link FieldMapper#parse(ParseContext)} on the {@link RootObjectMapper}.
     */
    public void postParse(ParseContext context) throws IOException {
        // do nothing
    }

}
