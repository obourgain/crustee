package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.table.Timestamped;

public interface ReadOnlyMemtable extends Timestamped {

    void applyInOrder(BiConsumer<ByteBuffer, Row> action);

    int getCount();

    Row get(ByteBuffer key);

    long getEstimatedSizeInBytes();

}
