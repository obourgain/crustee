package org.crustee.raft.storage.commitlog;

import java.nio.ByteBuffer;
import java.util.UUID;

public interface Segment extends AutoCloseable {
    void append(ByteBuffer buffer, int size);

    void append(ByteBuffer[] buffers, int length);

    boolean canWrite(int size);

    long sync();

    boolean isSynced();

    long getMaxSize();

    long getPosition();

    UUID getUuid();

    // do not throw exception
    @Override
    void close();

}
