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

package org.opensearch.action.admin.cluster.node.tasks.cancel;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.tasks.TransportTasksAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.function.Consumer;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask}
 *
 * @opensearch.internal
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, CancelTasksResponse, TaskInfo> {

    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(
            CancelTasksAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            CancelTasksRequest::new,
            CancelTasksResponse::new,
            TaskInfo::new,
            ThreadPool.Names.MANAGEMENT
        );
    }

    @Override
    protected CancelTasksResponse newResponse(
        CancelTasksRequest request,
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        return new CancelTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    protected void processTasks(CancelTasksRequest request, Consumer<CancellableTask> operation) {
        if (request.getTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            CancellableTask task = taskManager.getCancellableTask(request.getTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    operation.accept(task);
                } else {
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support this operation");
                }
            } else {
                if (taskManager.getTask(request.getTaskId().getId()) != null) {
                    // The task exists, but doesn't support cancellation
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support cancellation");
                } else {
                    throw new ResourceNotFoundException("task [{}] is not found", request.getTaskId());
                }
            }
        } else {
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task)) {
                    operation.accept(task);
                }
            }
        }
    }

    @Override
    protected void taskOperation(CancelTasksRequest request, CancellableTask cancellableTask, ActionListener<TaskInfo> listener) {
        String nodeId = clusterService.localNode().getId();
        taskManager.cancelTaskAndDescendants(
            cancellableTask,
            request.getReason(),
            request.waitForCompletion(),
            ActionListener.map(listener, r -> cancellableTask.taskInfo(nodeId, false))
        );
    }
}
