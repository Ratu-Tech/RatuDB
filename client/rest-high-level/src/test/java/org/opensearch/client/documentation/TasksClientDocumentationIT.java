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

package org.opensearch.client.documentation;

import org.opensearch.OpenSearchException;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.client.OpenSearchRestHighLevelClientTestCase;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.tasks.CancelTasksRequest;
import org.opensearch.client.tasks.CancelTasksResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This class is used to generate the Java Tasks API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 * <p>
 * Where example is your tag name.
 * <p>
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/{@link TasksClientDocumentationIT}.java[example]
 * --------------------------------------------------
 * <p>
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class TasksClientDocumentationIT extends OpenSearchRestHighLevelClientTestCase {

    @SuppressWarnings("unused")
    public void testListTasks() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::list-tasks-request
            ListTasksRequest request = new ListTasksRequest();
            // end::list-tasks-request

            // tag::list-tasks-request-filter
            request.setActions("cluster:*"); // <1>
            request.setNodes("nodeId1", "nodeId2"); // <2>
            request.setParentTaskId(new TaskId("parentTaskId", 42)); // <3>
            // end::list-tasks-request-filter

            // tag::list-tasks-request-detailed
            request.setDetailed(true); // <1>
            // end::list-tasks-request-detailed

            // tag::list-tasks-request-wait-completion
            request.setWaitForCompletion(true); // <1>
            request.setTimeout(TimeValue.timeValueSeconds(50)); // <2>
            request.setTimeout("50s"); // <3>
            // end::list-tasks-request-wait-completion
        }

        ListTasksRequest request = new ListTasksRequest();

        // tag::list-tasks-execute
        ListTasksResponse response = client.tasks().list(request, RequestOptions.DEFAULT);
        // end::list-tasks-execute

        assertThat(response, notNullValue());

        // tag::list-tasks-response-tasks
        List<TaskInfo> tasks = response.getTasks(); // <1>
        // end::list-tasks-response-tasks

        // tag::list-tasks-response-calc
        Map<String, List<TaskInfo>> perNodeTasks = response.getPerNodeTasks(); // <1>
        List<TaskGroup> groups = response.getTaskGroups(); // <2>
        // end::list-tasks-response-calc

        // tag::list-tasks-response-failures
        List<OpenSearchException> nodeFailures = response.getNodeFailures(); // <1>
        List<TaskOperationFailure> taskFailures = response.getTaskFailures(); // <2>
        // end::list-tasks-response-failures

        assertThat(response.getNodeFailures(), equalTo(emptyList()));
        assertThat(response.getTaskFailures(), equalTo(emptyList()));
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(2));
    }

    public void testListTasksAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ListTasksRequest request = new ListTasksRequest();

            // tag::list-tasks-execute-listener
            ActionListener<ListTasksResponse> listener =
                    new ActionListener<ListTasksResponse>() {
                        @Override
                        public void onResponse(ListTasksResponse response) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::list-tasks-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::list-tasks-execute-async
            client.tasks().listAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::list-tasks-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testCancelTasks() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::cancel-tasks-request
            CancelTasksRequest request = new org.opensearch.client.tasks.CancelTasksRequest.Builder()
                .withNodesFiltered(Arrays.asList("nodeId1", "nodeId2"))
                .withActionsFiltered(Collections.singletonList("cluster:*"))
                .build();
            // end::cancel-tasks-request

            // tag::cancel-tasks-request-filter
            CancelTasksRequest byTaskIdRequest = new org.opensearch.client.tasks.CancelTasksRequest.Builder() // <1>
                .withTaskId(new org.opensearch.client.tasks.TaskId("myNode",44L)) // <2>
                .withWaitForCompletion(true) // <3>
                .build(); // <4>
            // end::cancel-tasks-request-filter

        }

        CancelTasksRequest request = new org.opensearch.client.tasks.CancelTasksRequest.Builder().build();

        // tag::cancel-tasks-execute
        CancelTasksResponse response = client.tasks().cancel(request, RequestOptions.DEFAULT);
        // end::cancel-tasks-execute

        assertThat(response, notNullValue());

        // tag::cancel-tasks-response-tasks
        List<org.opensearch.client.tasks.TaskInfo> tasks = response.getTasks(); // <1>
        // end::cancel-tasks-response-tasks

        // tag::cancel-tasks-response-calc
        Map<String, List<org.opensearch.client.tasks.TaskInfo>> perNodeTasks = response.getPerNodeTasks(); // <1>
        List<org.opensearch.client.tasks.TaskGroup> groups = response.getTaskGroups(); // <2>
        // end::cancel-tasks-response-calc

        // tag::cancel-tasks-response-failures
        List<org.opensearch.client.tasks.OpenSearchException> nodeFailures = response.getNodeFailures(); // <1>
        List<org.opensearch.client.tasks.TaskOperationFailure> taskFailures = response.getTaskFailures(); // <2>
        // end::cancel-tasks-response-failures

        assertThat(response.getNodeFailures(), equalTo(emptyList()));
        assertThat(response.getTaskFailures(), equalTo(emptyList()));
    }

    public void testAsyncCancelTasks() throws InterruptedException {

        RestHighLevelClient client = highLevelClient();
        {
            CancelTasksRequest request = new org.opensearch.client.tasks.CancelTasksRequest.Builder().build();

            // tag::cancel-tasks-execute-listener
            ActionListener<CancelTasksResponse> listener =
                new ActionListener<CancelTasksResponse>() {
                    @Override
                    public void onResponse(CancelTasksResponse response) {
                        // <1>
                    }
                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::cancel-tasks-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::cancel-tasks-execute-async
            client.tasks().cancelAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::cancel-tasks-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
