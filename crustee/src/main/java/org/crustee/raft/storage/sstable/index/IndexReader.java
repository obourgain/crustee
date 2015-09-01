package org.crustee.raft.storage.sstable.index;

import java.io.Closeable;
import java.nio.ByteBuffer;
import org.crustee.raft.storage.sstable.RowLocation;

public interface IndexReader extends Closeable {

    RowLocation findRowLocation(ByteBuffer searchedKey);
}
