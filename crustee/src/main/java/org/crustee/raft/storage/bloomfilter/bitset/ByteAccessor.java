package org.crustee.raft.storage.bloomfilter.bitset;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public interface ByteAccessor {
    byte get(int index);
    void set(int index, byte value);
    int maxIndex();

    /**
     * @return the number of bytes written
     */
    long writeTo(WritableByteChannel channel);

    void readFrom(ReadableByteChannel channel, int size);
}
