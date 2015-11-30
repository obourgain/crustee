package org.crustee.raft.storage.sstable.index;

import java.nio.file.Path;

public class IndexReaderFactory {

    public static IndexReader create(Path index) {
        MmapIndexReader indexReader = new MmapIndexReader(index);
        TreeMapIndexSummary summary = new TreeMapIndexSummary(indexReader, 128);
        return new IndexWithSummary(indexReader, summary);
    }

}
