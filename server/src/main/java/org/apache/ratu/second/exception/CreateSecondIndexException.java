/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.ratu.second.exception;

public class CreateSecondIndexException extends BaseException {
    public CreateSecondIndexException(final String reason){
        super(reason);
    }
}
