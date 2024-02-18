package org.janusgraph.diskstorage.opensearch.rest.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.opensearch.client.RestClientBuilder;

public class BasicAuthHttpClientConfigCallback implements RestClientBuilder.HttpClientConfigCallback {

    private final CredentialsProvider credentialsProvider;

    public BasicAuthHttpClientConfigCallback(final String realm, final String username, final String password) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(username), "HTTP Basic Authentication: username must be provided");
        Preconditions.checkArgument(StringUtils.isNotEmpty(password), "HTTP Basic Authentication: password must be provided");

        credentialsProvider = new BasicCredentialsProvider();

        final AuthScope authScope;
        if (StringUtils.isNotEmpty(realm)) {
            authScope = new AuthScope(null,null, -1, realm,null);
        } else {
            authScope = new AuthScope(null,null, -1, null,null);
        }
//        credentialsProvider.setCredentials(authScope, new UsernamePasswordCredentials(username, password));
    }


    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        return httpClientBuilder;
    }
}
