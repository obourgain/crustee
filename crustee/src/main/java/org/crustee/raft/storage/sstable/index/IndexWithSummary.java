package org.crustee.raft.storage.sstable.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.crustee.raft.storage.sstable.RowLocation;

public class IndexWithSummary implements IndexReader {

    private final InternalIndexReader delegate;
    private final IndexSummary summary;

    IndexWithSummary(InternalIndexReader delegate, IndexSummary summary) {
        this.delegate = delegate;
        this.summary = summary;
    }

    public static long readCount = 0;

    @Override
    public RowLocation findRowLocation(ByteBuffer searchedKey) {
        int startAt = summary.previousIndexEntryLocation(searchedKey);
        readCount++;
        return delegate.findRowLocation(searchedKey, startAt, summary.getSamplingInterval());
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        // TODO close summary here if it someday comes to hold resources
    }
}
