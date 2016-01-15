package org.crustee.raft.storage.sstable.index;

import java.nio.ByteBuffer;

public interface IndexSummary {
    int previousIndexEntryLocation(ByteBuffer key);

    int getSamplingInterval();
}
