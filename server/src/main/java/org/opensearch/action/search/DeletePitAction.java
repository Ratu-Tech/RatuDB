/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Action type for deleting point in time searches
 */
public class DeletePitAction extends ActionType<DeletePitResponse> {

    public static final DeletePitAction INSTANCE = new DeletePitAction();
    public static final String NAME = "indices:data/read/point_in_time/delete";

    private DeletePitAction() {
        super(NAME, DeletePitResponse::new);
    }
}
