package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.Map;

public interface WritableMemtable extends ReadOnlyMemtable {

    /**
     * Modifications are isolated at the row level.
     */
    void insert(ByteBuffer rowKey, Map<ByteBuffer, ByteBuffer> values);

    long getEstimatedSizeInBytes();

    ReadOnlyMemtable freeze();
}
