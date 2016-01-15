package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;
import org.crustee.raft.storage.sstable.index.IndexWriter;

/**
 * Indicate where located the row is located in the table file, based on data from the index.
 */
public class RowLocation {

    public static final RowLocation NOT_FOUND = new RowLocation((short) -1, -1, -1);

    private static final int SERIALIZED_SIZE = IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH;

    private final short rowKeySize;
    private final long offset;
    private final int valueSize;

    public RowLocation(short rowKeySize, long offset, int valueSize) {
        this.rowKeySize = rowKeySize;
        this.offset = offset;
        this.valueSize = valueSize;
    }

    public short getRowKeySize() {
        return rowKeySize;
    }

    public long getRowKeyOffset() {
        return offset + 2 + 4; // keysize & value size
    }

    public long getValueOffset() {
        return offset + 2 + 4 + rowKeySize; // keysize + value size + key
    }

    public long getOffset() {
        return offset;
    }

    public int getValueSize() {
        return valueSize;
    }

    public boolean isFound() {
        return this != NOT_FOUND;
    }

    /**
     * Modifies the position of buffer.
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putShort(rowKeySize);
        buffer.putLong(offset);
        buffer.putInt(valueSize);
    }

    /**
     * Does not modifies the position of buffer.
     */
    public static RowLocation deserialize(ByteBuffer buffer, int startAt) {
        assert buffer.limit() - startAt >= SERIALIZED_SIZE : "" + buffer.limit() + " / " + startAt;
        short rowKeySize = buffer.getShort(startAt);
        long offset = buffer.getLong(startAt + Short.BYTES);
        int valueSize = buffer.getInt(startAt + Short.BYTES +  Long.BYTES);
        return new RowLocation(rowKeySize, offset, valueSize);
    }

    @Override
    public String toString() {
        return "RowLocation{" +
                "rowKeySize=" + rowKeySize +
                ", offset=" + offset +
                ", valueSize=" + valueSize +
                '}';
    }
}
