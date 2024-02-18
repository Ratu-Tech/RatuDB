package org.janusgraph.diskstorage.opensearch.rest;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import java.util.Map;

public class RestIndexSettings {

    private Settings settings;

    public Settings getSettings() {
        return settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public static class Settings {

        @JsonProperty("index")
        private Map<String,Object> map;

        public Map<String, Object> getMap() {
            return map;
        }

        public void setMap(Map<String, Object> map) {
            this.map = map;
        }
    }

}
