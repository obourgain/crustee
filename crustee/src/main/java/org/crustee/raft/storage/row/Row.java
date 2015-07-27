package org.crustee.raft.storage.row;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Implementations must be thread safe assuming single writer and multiple readers.
 */
public interface Row {

    void addAll(Map<ByteBuffer, ByteBuffer> entries);

    /**
     * Do not modify the returned map
     */
    Map<ByteBuffer, ByteBuffer> asMap();

}
