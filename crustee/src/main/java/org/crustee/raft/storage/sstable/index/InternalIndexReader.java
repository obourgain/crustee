package org.crustee.raft.storage.sstable.index;

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.crustee.raft.storage.sstable.RowLocation;

public interface InternalIndexReader extends Closeable {

    RowLocation findRowLocation(ByteBuffer searchedKey, int startAt, int maxScannedEntry);
}
