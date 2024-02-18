/*
* Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * This class handles the dependent extensions information
 *
 * @opensearch.internal
 */
public class ExtensionDependency implements Writeable {
    private String uniqueId;
    private Version version;
    private static final String UNIQUE_ID = "uniqueId";
    private static final String VERSION = "version";

    public ExtensionDependency(String uniqueId, Version version) {
        this.uniqueId = uniqueId;
        this.version = version;
    }

    /**
     * Jackson requires a no-arg constructor.
     *
     */
    @SuppressWarnings("unused")
    private ExtensionDependency() {}

    /**
    * Reads the extension dependency information
    *
    * @throws IOException if an I/O exception occurred reading the extension dependency information
    */
    public ExtensionDependency(StreamInput in) throws IOException {
        uniqueId = in.readString();
        version = in.readVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uniqueId);
        out.writeVersion(version);
    }

    /**
    * The uniqueId of the dependency extension
    *
    * @return the extension uniqueId
    */
    public String getUniqueId() {
        return uniqueId;
    }

    /**
    * The minimum version of the dependency extension
    *
    * @return the extension version
    */
    public Version getVersion() {
        return version;
    }

    public String toString() {
        return "ExtensionDependency:{uniqueId=" + uniqueId + ", version=" + version + "}";
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionDependency that = (ExtensionDependency) obj;
        return Objects.equals(uniqueId, that.uniqueId) && Objects.equals(version, that.version);
    }

    public int hashCode() {
        return Objects.hash(uniqueId, version);
    }
}
