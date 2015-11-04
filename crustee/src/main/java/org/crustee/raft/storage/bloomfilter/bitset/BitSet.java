package org.crustee.raft.storage.bloomfilter.bitset;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.crustee.raft.storage.bloomfilter.ByteAccessorFactory;
import org.crustee.raft.utils.UncheckedIOUtils;

public class BitSet {

    private final ByteAccessor byteAccessor;

    public BitSet(ByteAccessor byteAccessor) {
        this.byteAccessor = byteAccessor;
    }

    public long sizeInBits() {
        return byteAccessor.maxIndex() << 3;
    }

    public int sizeInBytes() {
        return byteAccessor.maxIndex();
    }

    public boolean get(long bitIndex) {
        int byteIndex = byteIndex(bitIndex);
        int bitIndexInByte = bitIndexInByte(bitIndex);
        int bitmask = 0x1 << bitIndexInByte;
        int b = byteAccessor.get(byteIndex);
        return (b & bitmask) != 0;
    }

    public void set(long bitIndex) {
        int byteIndex = byteIndex(bitIndex);
        int bitIndexInByte = bitIndexInByte(bitIndex);
        int bitmask = (byte) (0x1 << bitIndexInByte);
        int b = byteAccessor.get(byteIndex);
        b = b | bitmask;
        byteAccessor.set(byteIndex, (byte) b);
    }

    public void clear(long bitIndex) {
        int byteIndex = byteIndex(bitIndex);
        int bitIndexInByte = bitIndexInByte(bitIndex);
        int bitmask = (byte) (0x1 << bitIndexInByte);
        int b = byteAccessor.get(byteIndex);
        b = b & ~bitmask; // take the complement of the mask to unset the bit
        byteAccessor.set(byteIndex, (byte) b);
    }

    private int byteIndex(long bitIndex) {
        assert bitIndex <= Integer.MAX_VALUE * 8L : "index : " + bitIndex + " size (bits): " + sizeInBits();
        return (int) (bitIndex >> 3);
    }

    private int bitIndexInByte(long bitIndex) {
        return (int) (bitIndex & 0x7); // keep only lowest 8 bits
    }

    /**
     * @return the number of bytes written
     */
    public long writeTo(WritableByteChannel channel) {
        ByteBuffer size = (ByteBuffer) ByteBuffer.allocate(4).putInt(sizeInBytes()).flip();
        UncheckedIOUtils.write(channel, size);
        return byteAccessor.writeTo(channel);
    }

    public static BitSet readFrom(ReadableByteChannel channel) {
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        UncheckedIOUtils.read(channel, sizeBuffer);
        sizeBuffer.flip();
        int size = sizeBuffer.getInt();
        ByteAccessor byteAccessor = ByteAccessorFactory.newAccessor(size);
        byteAccessor.readFrom(channel, size);
        return new BitSet(byteAccessor);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BitSet bitSet = (BitSet) o;

        // do not use equals() because we may have different implementations and we are only interested in the content
        return byteAccessor.isContentEqual(bitSet.byteAccessor);

    }

    @Override
    public int hashCode() {
        return byteAccessor.hashCode();
    }
}
