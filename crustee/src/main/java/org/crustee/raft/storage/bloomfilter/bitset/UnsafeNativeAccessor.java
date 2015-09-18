package org.crustee.raft.storage.bloomfilter.bitset;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.crustee.raft.utils.UncheckedIOUtils;
import sun.misc.Unsafe;
import uk.co.real_logic.agrona.UnsafeAccess;

public final class UnsafeNativeAccessor implements ByteAccessor {

    private static final boolean DISABLE_BOUND_CHECK = Boolean.getBoolean("bitset.unsafe.boundcheck.disabled");

    private static final Unsafe unsafe = UnsafeAccess.UNSAFE;
    private final long baseAddress;
    private final int length;

    public UnsafeNativeAccessor(long baseAddress, int length) {
        if(!DirectByteBufferFactory.AVAILABLE) {
            throw new RuntimeException(UnsafeNativeAccessor.class.getSimpleName() + " is not available");
        }
        this.baseAddress = baseAddress;
        this.length = length;
    }

    @Override
    public final byte get(int index) {
        boundCheck(index);
        return unsafe.getByte(baseAddress + index);
    }

    @Override
    public final void set(int index, byte value) {
        boundCheck(index);
        unsafe.putByte(baseAddress + index, value);
    }

    @Override
    public int maxIndex() {
        return length;
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        ByteBuffer byteBuffer = DirectByteBufferFactory.wrap(baseAddress, length);
        return UncheckedIOUtils.write(channel, byteBuffer);
    }

    @Override
    public void readFrom(ReadableByteChannel channel, int size) {
        ByteBuffer byteBuffer = DirectByteBufferFactory.wrap(baseAddress, length);
        UncheckedIOUtils.copy(channel, byteBuffer);
    }

    private void boundCheck(int index) {
        if(!DISABLE_BOUND_CHECK && (index > length || index < 0)) {
            throw new IndexOutOfBoundsException("tried to access index " + index + " on buffer of size " + length);
        }
    }
}
