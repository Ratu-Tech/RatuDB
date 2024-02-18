package org.janusgraph.diskstorage.opensearch;

import org.janusgraph.diskstorage.indexing.RawQuery.Result;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class OpenSearchScroll implements Iterator<Result<String>> {

    private final BlockingQueue<Result<String>> queue;
    private final OpenSearchClient client;
    private final int batchSize;

    private boolean isFinished;
    private String scrollId;

    public OpenSearchScroll(OpenSearchClient client, OpenSearchResponse initialResponse, int nbDocByQuery) {
        queue = new LinkedBlockingQueue<>();
        this.client = client;
        this.batchSize = nbDocByQuery;
        update(initialResponse);
    }

    private void update(OpenSearchResponse response) {
        response.getResults().forEach(queue::add);
        this.scrollId = response.getScrollId();
        this.isFinished = response.numResults() < this.batchSize;
        try {
            if (isFinished) client.deleteScroll(scrollId);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (!queue.isEmpty()) {
                return true;
            }
            if (isFinished) {
                return false;
            }
            final OpenSearchResponse res = client.search(scrollId);
            update(res);
            return res.numResults() > 0;
        } catch (final IOException e) {
             throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    @Override
    public Result<String> next() {
        if (hasNext()) {
            return queue.remove();
        }
        throw new NoSuchElementException();
    }
}
