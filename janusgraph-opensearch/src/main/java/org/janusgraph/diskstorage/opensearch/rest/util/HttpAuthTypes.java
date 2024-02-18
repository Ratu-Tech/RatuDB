package org.janusgraph.diskstorage.opensearch.rest.util;

public enum HttpAuthTypes {
    /** No authentication - default */
    NONE,
    /** Basic authentication with username/password */
    BASIC,
    /** Custom authentication with a provided authenticator implementation. Please refer to the documentation for
     * the custom authenticator.
     */
    CUSTOM
}
