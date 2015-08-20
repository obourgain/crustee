package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import org.crustee.raft.storage.row.Row;

public interface ReadOnlyMemtable {

    void applyInOrder(BiConsumer<ByteBuffer, Row> action);

    int getCount();

    Row get(ByteBuffer key);

    long getEstimatedSizeInBytes();
}
