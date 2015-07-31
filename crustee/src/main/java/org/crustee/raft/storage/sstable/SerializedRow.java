package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;

public class SerializedRow {

    private final ByteBuffer[] buffers;

    public SerializedRow(ByteBuffer... buffers) {
        this.buffers = buffers;
    }

    public ByteBuffer[] getBuffers() {
        return buffers;
    }

    public int totalSize() {
        int total = 0;
        for (ByteBuffer buffer : buffers) {
            total += buffer.limit();
        }
        return total;
    }
}
