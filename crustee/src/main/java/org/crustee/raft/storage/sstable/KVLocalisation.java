package org.crustee.raft.storage.sstable;

public class KVLocalisation {

    public static final KVLocalisation NOT_FOUND = new KVLocalisation((short) -1, -1, -1);

    private final short keySize;
    private final long keyOffset;
    private final long valueOffset;
    private final int valueSize;

    public KVLocalisation(short keySize, long valueOffset, int valueSize) {
        this.keySize = keySize;
        this.keyOffset = valueOffset - keySize;
        this.valueOffset = valueOffset;
        this.valueSize = valueSize;
    }

    public short getKeySize() {
        return keySize;
    }

    public long getKeyOffset() {
        return keyOffset;
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
                "keySize=" + keySize +
                ", keyOffset=" + keyOffset +
                ", valueOffset=" + valueOffset +
                ", valueSize=" + valueSize +
                '}';
    }
}
