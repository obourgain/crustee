package org.crustee.raft.storage.bloomfilter;

import java.nio.ByteBuffer;

public interface MutableBloomFilter extends ReadOnlyBloomFilter {
    void add(ByteBuffer buffer);
}
