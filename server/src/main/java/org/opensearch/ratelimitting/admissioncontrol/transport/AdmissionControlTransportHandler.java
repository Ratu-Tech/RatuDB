/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * AdmissionControl Handler to intercept Transport Requests.
 * @param <T> Transport Request
 */
public class AdmissionControlTransportHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

    private final String action;
    private final TransportRequestHandler<T> actualHandler;
    protected final Logger log = LogManager.getLogger(this.getClass());
    AdmissionControlService admissionControlService;
    boolean forceExecution;

    public AdmissionControlTransportHandler(
        String action,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlService admissionControlService,
        boolean forceExecution
    ) {
        super();
        this.action = action;
        this.actualHandler = actualHandler;
        this.admissionControlService = admissionControlService;
        this.forceExecution = forceExecution;
    }

    /**
     * @param request Transport Request that landed on the node
     * @param channel Transport channel allows to send a response to a request
     * @param task Current task that is executing
     * @throws Exception when admission control rejected the requests
     */
    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        // intercept all the transport requests here and apply admission control
        try {
            // TODO Need to evaluate if we need to apply admission control or not if force Execution is true will update in next PR.
            this.admissionControlService.applyTransportAdmissionControl(this.action);
        } catch (final OpenSearchRejectedExecutionException openSearchRejectedExecutionException) {
            log.warn(openSearchRejectedExecutionException.getMessage());
            channel.sendResponse(openSearchRejectedExecutionException);
        } catch (final Exception e) {
            throw e;
        }
        actualHandler.messageReceived(request, channel, task);
    }
}
