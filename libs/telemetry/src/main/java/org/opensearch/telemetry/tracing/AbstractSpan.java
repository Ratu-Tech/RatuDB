/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;

/**
 * Base span
 *
 * @opensearch.internal
 */
@InternalApi
public abstract class AbstractSpan implements Span {

    /**
     * name of the span
     */
    private final String spanName;
    /**
     * span's parent span
     */
    private final Span parentSpan;

    /**
     * Base constructor
     * @param spanName name of the span
     * @param parentSpan span's parent span
     */
    protected AbstractSpan(String spanName, Span parentSpan) {
        this.spanName = spanName;
        this.parentSpan = parentSpan;
    }

    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    @Override
    public String getSpanName() {
        return spanName;
    }

}
