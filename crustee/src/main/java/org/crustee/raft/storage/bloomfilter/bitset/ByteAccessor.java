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

    default boolean isContentEqual(ByteAccessor other) {
        if(maxIndex() != other.maxIndex()) {
            return false;
        }
        for (int i = 0; i < maxIndex(); i++) {
            if(this.get(i) != other.get(i)) {
                return false;
            }
        }
        return true;
    }
}
