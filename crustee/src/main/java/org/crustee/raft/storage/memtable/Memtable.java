package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiConsumer;
import org.crustee.raft.storage.row.Row;

public interface Memtable {

    void applyInOrder(BiConsumer<ByteBuffer, Row> action);

    int getCount();

    Row get(ByteBuffer key);

    /**
     * Modifications are isolated at the row level.
     */
    void insert(ByteBuffer rowKey, Map<ByteBuffer, ByteBuffer> values);

    long getEstimatedSizeInBytes();

    void freeze();
}
