package org.crustee.raft.storage.sstable;

/**
 * Indicate where located the row is located in the table file, based on data from the index.
 */
public class RowLocation {

    public static final RowLocation NOT_FOUND = new RowLocation((short) -1, -1, -1);

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

    @Override
    public String toString() {
        return "KVLocalisation{" +
                "rowKeySize=" + rowKeySize +
                ", valueSize=" + valueSize +
                ", offset=" + offset +
                '}';
    }
}
