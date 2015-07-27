package org.crustee.raft.storage.sstable;

/**
 * Indicated where is located the value in the table file, based on data from the index.
 */
public class KVLocation {

    public static final KVLocation NOT_FOUND = new KVLocation((short) -1, -1, -1);

    private final short rowKeySize;
    private final long rowKeyOffset;
    private final long valueOffset;
    private final int valueSize;

    public KVLocation(short rowKeySize, long valueOffset, int valueSize) {
        this.rowKeySize = rowKeySize;
        this.rowKeyOffset = valueOffset - rowKeySize;
        this.valueOffset = valueOffset;
        this.valueSize = valueSize;
    }

    public short getRowKeySize() {
        return rowKeySize;
    }

    public long getRowKeyOffset() {
        return rowKeyOffset;
    }

    public long getValueOffset() {
        return valueOffset;
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
                ", rowKeyOffset=" + rowKeyOffset +
                ", valueOffset=" + valueOffset +
                ", valueSize=" + valueSize +
                '}';
    }
}
