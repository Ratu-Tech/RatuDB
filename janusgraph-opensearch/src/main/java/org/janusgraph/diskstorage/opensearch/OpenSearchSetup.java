package org.janusgraph.diskstorage.opensearch;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.opensearch.rest.RestClientSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum OpenSearchSetup {

    /**
     * Create an ES RestClient connected to
     * {@link org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration#INDEX_HOSTS}.
     */
    REST_CLIENT {
        @Override
        public Connection connect(Configuration config) throws IOException {
            return new Connection(new RestClientSetup().connect(config));
        }
    };

    @SuppressWarnings("unchecked")
    static Map<String, Object> getSettingsFromJanusGraphConf(Configuration config) {

        final Map<String, Object> settings = new HashMap<>();

        int keysLoaded = 0;
        final Map<String,Object> configSub = config.getSubset(OpenSearchIndex.ES_CREATE_EXTRAS_NS);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (null == val) continue;
            if (List.class.isAssignableFrom(val.getClass())) {
                // Pretty print lists using comma-separated values and no surrounding square braces for ES
                List<?> l = (List<?>) val;
                settings.put(key, Joiner.on(",").join(l));
            } else if (val.getClass().isArray()) {
                // As with Lists, but now for arrays
                // The Object copy[] business lets us avoid repetitive primitive array type checking and casting
                Object[] copy = new Object[Array.getLength(val)];
                for (int i= 0; i < copy.length; i++) {
                    copy[i] = Array.get(val, i);
                }
                settings.put(key, Joiner.on(",").join(copy));
            } else {
                // Copy anything else unmodified
                settings.put(key, val.toString());
            }
            log.debug("[ES ext.* cfg] Set {}: {}", key, val);
            keysLoaded++;
        }
        log.debug("Loaded {} settings from the {} JanusGraph config namespace", keysLoaded, OpenSearchIndex.ES_CREATE_EXTRAS_NS);
        return settings;
    }

    private static final Logger log = LoggerFactory.getLogger(OpenSearchSetup.class);

    public abstract Connection connect(Configuration config) throws IOException;

    public static class Connection {

        private final OpenSearchClient client;

        public Connection(OpenSearchClient client) {
            this.client = Preconditions.checkNotNull(client, "Unable to instantiate Opensearch Client object");
        }

        public OpenSearchClient getClient() {
            return client;
        }
    }
}
