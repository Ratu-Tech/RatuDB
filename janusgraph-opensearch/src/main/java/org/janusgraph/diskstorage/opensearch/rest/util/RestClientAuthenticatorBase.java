package org.janusgraph.diskstorage.opensearch.rest.util;


import org.apache.commons.lang3.builder.Builder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;

import java.io.IOException;

public abstract class RestClientAuthenticatorBase implements RestClientAuthenticator {


    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        return httpClientBuilder;
    }


    public Builder<Object> customizeRequestConfig(Builder<Object> requestConfigBuilder) {
        return requestConfigBuilder;
    }

    @Override
    public void init() throws IOException {
        // does nothing
    }
}
