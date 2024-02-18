package org.janusgraph.diskstorage.opensearch.rest.util;

import org.janusgraph.diskstorage.opensearch.OpenSearchIndex;
import org.opensearch.client.RestClientBuilder;

import java.io.IOException;


public interface RestClientAuthenticator extends RestClientBuilder.HttpClientConfigCallback, RestClientBuilder.RequestConfigCallback {

    /**
     * Initializes the authenticator. This method may perform the blocking I/O operations if needed.
     * @throws IOException in case there was an exception during I/O operations.
     */
    void init() throws IOException;

}
