package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;

public class SSTableHeader {

    public static final int BUFFER_SIZE = 8 + /* entryCount */ 8 /* size */;

    private final long entryCount;
    private long size = -1; // overwritten in writeCompletedHeader with real size

    private boolean completed = false;

    public SSTableHeader(long entryCount) {
        this.entryCount = entryCount;
    }

    public SSTableHeader complete(long size) {
        this.size = size;
        this.completed = true;
        return this;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public long getSize() {
        return size;
    }

    public ByteBuffer asTemporaryByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE)
                .putLong(entryCount)
                .putLong(size); // -1 at this point
        buffer.flip();
        return buffer;
    }

    public ByteBuffer asCompletedByteBuffer() {
        assert completed;
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE)
                .putLong(entryCount)
                .putLong(size);
        buffer.flip();
        return buffer;
    }

    public static SSTableHeader fromBuffer(ByteBuffer buffer) {
        assert buffer.capacity() == BUFFER_SIZE;
        long entryCount = buffer.getLong(0);
        long size = buffer.getLong(8);
        return new SSTableHeader(entryCount).complete(size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SSTableHeader that = (SSTableHeader) o;

        if (entryCount != that.entryCount) return false;
        if (size != that.size) return false;
        return completed == that.completed;

    }

    @Override
    public int hashCode() {
        int result = (int) (entryCount ^ (entryCount >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (completed ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SSTableHeader{" +
                "entryCount=" + entryCount +
                ", size=" + size +
                ", completed=" + completed +
                '}';
    }
}
