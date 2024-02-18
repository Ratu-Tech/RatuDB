package org.janusgraph.diskstorage.opensearch.rest.util;

import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.client.RestClientBuilder;

public class ConnectionKeepAliveConfigCallback implements RestClientBuilder.HttpClientConfigCallback {

    private final long keepAliveDuration;

    /**
     * Constructor
     * @param keepAliveDuration The keep-alive duration in milliseconds
     */
    public ConnectionKeepAliveConfigCallback(long keepAliveDuration) {
        this.keepAliveDuration = keepAliveDuration;
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        httpClientBuilder.setKeepAliveStrategy((a, b) -> Timeout.ofMilliseconds(this.keepAliveDuration));
        return httpClientBuilder;
    }
}
