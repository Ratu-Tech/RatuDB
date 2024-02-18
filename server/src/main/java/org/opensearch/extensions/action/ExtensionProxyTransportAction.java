/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * A proxy transport action used to proxy a transport request from OpenSearch or a plugin to execute on an extension
 *
 * @opensearch.internal
 */
public class ExtensionProxyTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private final ExtensionsManager extensionsManager;

    @Inject
    public ExtensionProxyTransportAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ExtensionsManager extensionsManager
    ) {
        super(ExtensionProxyAction.NAME, transportService, actionFilters, ExtensionActionRequest::new);
        this.extensionsManager = extensionsManager;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> listener) {
        try {
            listener.onResponse(extensionsManager.handleTransportRequest(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
