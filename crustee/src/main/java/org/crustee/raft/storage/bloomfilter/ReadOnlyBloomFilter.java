package org.crustee.raft.storage.bloomfilter;

import java.nio.ByteBuffer;

public interface ReadOnlyBloomFilter {
    boolean mayBePresent(ByteBuffer key);
}
