package org.crustee.raft.storage.bloomfilter.bitset;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.crustee.raft.utils.UncheckedIOUtils;

public final class ByteBufferAccessor implements ByteAccessor {

    private final ByteBuffer byteBuffer;

    public ByteBufferAccessor(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public final byte get(int index) {
        return byteBuffer.get(index);
    }

    @Override
    public final void set(int index, byte value) {
        byteBuffer.put(index, value);
    }

    @Override
    public int maxIndex() {
        return byteBuffer.limit();
    }

    @Override
    public long writeTo(WritableByteChannel channel) {
        int written = UncheckedIOUtils.write(channel, byteBuffer);
        byteBuffer.flip(); // prepare to read again
        return written;
    }

    @Override
    public void readFrom(ReadableByteChannel channel, int size) {
        UncheckedIOUtils.copy(channel, byteBuffer);
    }

}
