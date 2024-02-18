/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratu.sstable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BulkLoader {

    private static final Logger logger = LoggerFactory.getLogger(BulkLoader.class);

    public static void main(String[] args) {
        try {
            org.apache.cassandra.tools.BulkLoader.main(args);
        } catch (Exception e) {
            logger.error("BulkLoader Data Exception:",e);
        }
    }
}
