package org.crustee.raft.storage.sstable.index;

import java.nio.file.Path;

public class IndexReaderFactory {

    public static IndexReader create(Path index) {
        return new MmapIndexReader(index);
    }

}
