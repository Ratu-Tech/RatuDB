package org.janusgraph.diskstorage.opensearch.compat;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.opensearch.OpenMajorVersion;

public class ESCompatUtils {

    public static AbstractESCompat acquireCompatForVersion(OpenMajorVersion elasticMajorVersion) throws BackendException {
        switch (elasticMajorVersion) {
            case THREE:
                return new OS3Compat();
            default:
                throw new PermanentBackendException("Unsupported Opensearch version: " + elasticMajorVersion);
        }
    }
}
