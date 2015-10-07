package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.Map;
import org.crustee.raft.storage.commitlog.Segment;

public interface WritableMemtable extends ReadOnlyMemtable {

    /**
     * Modifications are isolated at the row level.
     */
    void insert(ByteBuffer rowKey, Map<ByteBuffer, ByteBuffer> values);

    void addSegment(Segment segment);

    ReadOnlyMemtable freeze();
}
