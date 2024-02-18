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

package org.opensearch.client;

import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.client.tasks.CancelTasksRequest;
import org.opensearch.client.tasks.CancelTasksResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.reindex.ReindexRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class TasksIT extends OpenSearchRestHighLevelClientTestCase {

    public void testListTasks() throws IOException {
        ListTasksRequest request = new ListTasksRequest();
        ListTasksResponse response = execute(request, highLevelClient().tasks()::list, highLevelClient().tasks()::listAsync);

        assertThat(response, notNullValue());
        assertThat(response.getNodeFailures(), equalTo(emptyList()));
        assertThat(response.getTaskFailures(), equalTo(emptyList()));
        // It's possible that there are other tasks except 'cluster:monitor/tasks/lists[n]' and 'action":"cluster:monitor/tasks/lists'
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(2));
        boolean listTasksFound = false;
        for (TaskGroup taskGroup : response.getTaskGroups()) {
            org.opensearch.tasks.TaskInfo parent = taskGroup.getTaskInfo();
            if ("cluster:monitor/tasks/lists".equals(parent.getAction())) {
                assertThat(taskGroup.getChildTasks().size(), equalTo(1));
                TaskGroup childGroup = taskGroup.getChildTasks().iterator().next();
                assertThat(childGroup.getChildTasks().isEmpty(), equalTo(true));
                org.opensearch.tasks.TaskInfo child = childGroup.getTaskInfo();
                assertThat(child.getAction(), equalTo("cluster:monitor/tasks/lists[n]"));
                assertThat(child.getParentTaskId(), equalTo(parent.getTaskId()));
                listTasksFound = true;
            }
        }
        assertTrue("List tasks were not found", listTasksFound);
    }

    public void testGetValidTask() throws Exception {

        // Run a Reindex to create a task
        final String sourceIndex = "source1";
        final String destinationIndex = "dest";
        Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        createIndex(sourceIndex, settings);
        createIndex(destinationIndex, settings);
        BulkRequest bulkRequest = new BulkRequest().add(
            new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), MediaTypeRegistry.JSON)
        )
            .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo2", "bar2"), MediaTypeRegistry.JSON))
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        assertEquals(RestStatus.OK, highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status());

        final ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices(sourceIndex).setDestIndex(destinationIndex);
        final TaskSubmissionResponse taskSubmissionResponse = highLevelClient().submitReindexTask(reindexRequest, RequestOptions.DEFAULT);

        final String taskId = taskSubmissionResponse.getTask();
        assertNotNull(taskId);

        TaskId childTaskId = new TaskId(taskId);
        GetTaskRequest gtr = new GetTaskRequest(childTaskId.getNodeId(), childTaskId.getId());
        gtr.setWaitForCompletion(randomBoolean());
        Optional<GetTaskResponse> getTaskResponse = execute(gtr, highLevelClient().tasks()::get, highLevelClient().tasks()::getAsync);
        assertTrue(getTaskResponse.isPresent());
        GetTaskResponse taskResponse = getTaskResponse.get();
        if (gtr.getWaitForCompletion()) {
            assertTrue(taskResponse.isCompleted());
        }
        org.opensearch.tasks.TaskInfo info = taskResponse.getTaskInfo();
        assertTrue(info.isCancellable());
        assertEquals("reindex from [source1] to [dest]", info.getDescription());
        assertEquals("indices:data/write/reindex", info.getAction());
        if (taskResponse.isCompleted() == false) {
            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }

    public void testGetInvalidTask() throws IOException {
        // Check 404s are returned as empty Optionals
        GetTaskRequest gtr = new GetTaskRequest("doesNotExistNodeName", 123);
        Optional<GetTaskResponse> getTaskResponse = execute(gtr, highLevelClient().tasks()::get, highLevelClient().tasks()::getAsync);
        assertFalse(getTaskResponse.isPresent());
    }

    public void testCancelTasks() throws IOException {
        ListTasksRequest listRequest = new ListTasksRequest();
        ListTasksResponse listResponse = execute(listRequest, highLevelClient().tasks()::list, highLevelClient().tasks()::listAsync);
        // in this case, probably no task will actually be cancelled.
        // this is ok, that case is covered in TasksIT.testTasksCancellation
        org.opensearch.tasks.TaskInfo firstTask = listResponse.getTasks().get(0);
        String node = listResponse.getPerNodeTasks().keySet().iterator().next();

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest.Builder().withTaskId(new TaskId(node, firstTask.getId())).build();
        CancelTasksResponse response = execute(
            cancelTasksRequest,
            highLevelClient().tasks()::cancel,
            highLevelClient().tasks()::cancelAsync
        );
        // Since the task may or may not have been cancelled, assert that we received a response only
        // The actual testing of task cancellation is covered by TasksIT.testTasksCancellation
        assertThat(response, notNullValue());
    }
}
