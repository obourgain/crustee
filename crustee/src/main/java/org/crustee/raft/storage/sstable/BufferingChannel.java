package org.crustee.raft.storage.sstable;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.utils.ByteBufferUtils;

public class BufferingChannel implements GatheringByteChannel, Flushable {

    @VisibleForTesting
    static final int BUFFERED_CHANNEL_BUFFER_SIZE = 256;

    private final FileChannel delegate;
    @VisibleForTesting
    final ByteBuffer[] buffer = new ByteBuffer[BUFFERED_CHANNEL_BUFFER_SIZE];
    @VisibleForTesting
    int bufferIndex = 0;
    private final TemporaryMetadataBufferSlab temporaryMetadataBufferSlab;

    // this class is highly specific to the use case here because we have the buffering over
    // a channel but also a cache of pre allocated buffers of a specific size, used here to
    // write the fixed-size part of the entries
    BufferingChannel(FileChannel delegate, int preallocatedBufferSize) {
        this.delegate = delegate;
        this.temporaryMetadataBufferSlab = new TemporaryMetadataBufferSlab(BUFFERED_CHANNEL_BUFFER_SIZE, preallocatedBufferSize);
    }

    public ByteBuffer getPreallocatedBuffer() {
        return temporaryMetadataBufferSlab.get();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long sum = 0;
        for (int i = offset; i < offset + length; i++) {
            sum += write(srcs[i]);
        }
        return sum;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int limit = src.limit();
        buffer[bufferIndex] = src;
        bufferIndex++;
        // we must flush after inserting the entry because we hold a reference to the metadata slab that would be cleared on flush
        if (bufferIndex == buffer.length) {
            flush();
        }
        return limit;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    public void flush() throws IOException {
        if (bufferIndex != 0) {
            delegate.write(buffer, 0, bufferIndex);
            bufferIndex = 0; // no need to clear the array as the ByteBuffers are already referenced in the memtable, so no garbage is kept
            temporaryMetadataBufferSlab.reset();
        }
    }

    @Override
    public void close() throws IOException {
        if (bufferIndex != 0) {
            throw new IllegalStateException("Write buffer have not been flushed before closing it");
        }
        delegate.close();
        temporaryMetadataBufferSlab.close();
    }

    @VisibleForTesting
    static class TemporaryMetadataBufferSlab {

        // allocate a single chunk of direct memory and slice it in small parts for the metadata of each of the entries to write

        final ByteBuffer[] buffers;
        int index = 0;
        final MappedByteBuffer slab;

        private boolean closed = false;

        TemporaryMetadataBufferSlab(int count, int size) {
            this.buffers = new ByteBuffer[count];
            slab = (MappedByteBuffer) ByteBuffer.allocateDirect(size * count);
            for (int i = 0; i < buffers.length; i++) {
                slab.position(i * size);
                buffers[i] = (ByteBuffer) slab.slice().limit(size);
            }
        }

        ByteBuffer get() {
            assert !closed;
            assert index < buffers.length;
            return buffers[index++];
        }

        void reset() {
            assert !closed;
            index = 0;
            for (ByteBuffer buffer : buffers) {
                buffer.position(0);
            }
        }

        void close() {
            Arrays.fill(buffers, null);
            ByteBufferUtils.tryUnmap(slab);
            closed = true;
        }
    }
}
