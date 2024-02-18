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

package org.opensearch.action.ingest;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.Pipeline;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PutPipelineRequestTests extends OpenSearchTestCase {

    public void testSerializationWithXContent() throws IOException {
        PutPipelineRequest request = new PutPipelineRequest(
            "1",
            new BytesArray("{}".getBytes(StandardCharsets.UTF_8)),
            MediaTypeRegistry.JSON
        );
        assertEquals(MediaTypeRegistry.JSON, request.getMediaType());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        PutPipelineRequest serialized = new PutPipelineRequest(in);
        assertEquals(MediaTypeRegistry.JSON, serialized.getMediaType());
        assertEquals("{}", serialized.getSource().utf8ToString());
    }

    public void testToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder pipelineBuilder = XContentBuilder.builder(xContentType.xContent());
        pipelineBuilder.startObject().field(Pipeline.DESCRIPTION_KEY, "some random set of processors");
        pipelineBuilder.startArray(Pipeline.PROCESSORS_KEY);
        // Start first processor
        pipelineBuilder.startObject();
        pipelineBuilder.startObject("set");
        pipelineBuilder.field("field", "foo");
        pipelineBuilder.field("value", "bar");
        pipelineBuilder.endObject();
        pipelineBuilder.endObject();
        // End first processor
        pipelineBuilder.endArray();
        pipelineBuilder.endObject();
        PutPipelineRequest request = new PutPipelineRequest("1", BytesReference.bytes(pipelineBuilder), xContentType);
        XContentBuilder requestBuilder = XContentBuilder.builder(xContentType.xContent());
        BytesReference actualRequestBody = BytesReference.bytes(request.toXContent(requestBuilder, ToXContent.EMPTY_PARAMS));
        assertEquals(BytesReference.bytes(pipelineBuilder), actualRequestBody);
    }
}
