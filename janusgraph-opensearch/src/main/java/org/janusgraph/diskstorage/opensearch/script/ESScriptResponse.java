package org.janusgraph.diskstorage.opensearch.script;

import org.apache.tinkerpop.shaded.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ESScriptResponse {

    private Boolean found;

    private ESScript script;

    public Boolean getFound() {
        return found;
    }

    public void setFound(Boolean found) {
        this.found = found;
    }

    public ESScript getScript() {
        return script;
    }

    public void setScript(ESScript script) {
        this.script = script;
    }
}
