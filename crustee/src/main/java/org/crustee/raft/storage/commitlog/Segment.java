package org.crustee.raft.storage.commitlog;

import java.nio.ByteBuffer;
import java.util.UUID;

public interface Segment {
    void append(ByteBuffer buffer);

    boolean canWrite(int size);

    long sync();

    boolean isSynced();

    long getMaxSize();

    long getPosition();

    UUID getUuid();

    void acquire();

    void release();

    boolean isClosed();
}
