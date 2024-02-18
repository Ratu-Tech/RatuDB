/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DecommissionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String attributeName = "zone";
        String attributeValue = "zone-1";
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        final DecommissionRequest originalRequest = new DecommissionRequest(decommissionAttribute);

        final DecommissionRequest deserialized = copyWriteable(originalRequest, writableRegistry(), DecommissionRequest::new);

        assertEquals(deserialized.getDecommissionAttribute(), originalRequest.getDecommissionAttribute());
        assertEquals(deserialized.getDelayTimeout(), originalRequest.getDelayTimeout());
    }

    public void testValidation() {
        {
            String attributeName = null;
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute name is missing"));
        }
        {
            String attributeName = "zone";
            String attributeValue = "";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute value is missing"));
        }
        {
            String attributeName = "zone";
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
            request.setNoDelay(true);
            ActionRequestValidationException e = request.validate();
            assertNull(e);
            assertEquals(TimeValue.ZERO, request.getDelayTimeout());
        }
        {
            String attributeName = "zone";
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
            ActionRequestValidationException e = request.validate();
            assertNull(e);
            assertEquals(DecommissionRequest.DEFAULT_NODE_DRAINING_TIMEOUT, request.getDelayTimeout());
        }
        {
            String attributeName = "zone";
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute);
            request.setNoDelay(true);
            request.setDelayTimeout(TimeValue.timeValueSeconds(30));
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("Invalid decommission request"));
        }
    }
}
