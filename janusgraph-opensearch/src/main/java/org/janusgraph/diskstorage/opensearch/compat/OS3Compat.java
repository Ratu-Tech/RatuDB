package org.janusgraph.diskstorage.opensearch.compat;

import org.janusgraph.diskstorage.indexing.IndexFeatures;

import static org.janusgraph.diskstorage.opensearch.OpenSearchConstants.CUSTOM_ALL_FIELD;


public class OS3Compat extends AbstractESCompat {

    private static final IndexFeatures FEATURES = coreFeatures().setWildcardField(CUSTOM_ALL_FIELD).supportsGeoContains().build();

    @Override
    public IndexFeatures getIndexFeatures() {
        return FEATURES;
    }
}
