package org.crustee.raft.storage.sstable;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.crustee.raft.utils.UncheckedIOUtils.fsync;
import static org.crustee.raft.utils.UncheckedIOUtils.fsyncDir;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.position;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.file.Path;
import java.util.Iterator;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.bloomfilter.BloomFilter;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.LongCursor;

public class SSTableWriter implements AutoCloseable {

    private static final Logger logger = getLogger(SSTableWriter.class);

    private static final int TABLE_ENTRY_KEY_VALUE_LENGTH = Short.BYTES + Integer.BYTES;
    private static final int INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH = Short.BYTES + Long.BYTES + Integer.BYTES;

    private final Path table;
    private final Path index;
    private final ReadOnlyMemtable memtable;

    private final FileChannel indexChannel;
    private final FileChannel tableChannel;

    private final LongArrayList valuesOffsets;
    private final IntArrayList entriesSize;

    protected SSTableHeader header;
    protected State completedState = State.NOTHING;

    private long offset = 0;

    private final BloomFilter bloomFilter;
    private final BufferingChannel tableBuffer;
    private final BufferingChannel indexBuffer;

    public SSTableWriter(Path table, Path index, ReadOnlyMemtable memtable) {
        this.table = table;
        this.index = index;
        this.memtable = memtable;
        this.valuesOffsets = new LongArrayList(memtable.getCount());
        this.entriesSize = new IntArrayList(memtable.getCount());

        this.tableChannel = openChannel(table, WRITE);
        this.indexChannel = openChannel(index, WRITE);

        tableBuffer = new BufferingChannel(tableChannel, TABLE_ENTRY_KEY_VALUE_LENGTH);
        indexBuffer = new BufferingChannel(indexChannel, INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH);

        this.header = new SSTableHeader(memtable.getCount(), memtable.getCreationTimestamp());
        this.bloomFilter = BloomFilter.create(memtable.getCount(), 0.01d);
    }

    public SSTableReader toReader() {
        // it would be better with only CLOSED, but with SYNCED it is easier to use with
        // try-with-resource on the caller side
        assert completedState == State.CLOSED | completedState == State.SYNCED;
        return new SSTableReader(table, index, bloomFilter);
    }

    public void write() {
        writeTemporaryHeader();
        completedState = State.TEMPORAY_HEADER;
        writeTable();
        completedState = State.TABLE;
        writeIndex();
        completedState = State.INDEX;

        // We flush right now because if a sstable reader is created from this writer and exposed to
        // reader threads, the flush may not have yet occurred. Furthermore, we need to
        // flush before fsync'ing.
        UncheckedIOUtils.flush(tableBuffer);
        UncheckedIOUtils.flush(indexBuffer);

        writeCompletedHeader();
        completedState = State.COMPLETE_HEADER;

        // put the bloom filter next to the index
        Path bloomFilterPath = index.resolveSibling(index.getFileName() + ".bf");
        UncheckedIOUtils.createFile(bloomFilterPath);
        try (FileChannel bloomFilterChannel = openChannel(bloomFilterPath, WRITE)) {
            bloomFilter.writeTo(bloomFilterChannel);
            fsync(bloomFilterChannel, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        fsync(tableChannel, true);
        fsync(indexChannel, true);
        Path tableDir = table.getParent();
        Path indexDir = index.getParent();
        fsyncDir(tableDir);
        if (!indexDir.equals(tableDir)) {
            fsyncDir(indexDir);
        }
        completedState = State.SYNCED;
    }

    protected void writeTemporaryHeader() {
        assert completedState == State.NOTHING;
        ByteBuffer header = this.header.asTemporaryByteBuffer();
        UncheckedIOUtils.write(tableChannel, header); // updates position in the channel
        offset += SSTableHeader.BUFFER_SIZE;
    }

    protected void writeCompletedHeader() {
        assert completedState == State.INDEX;
        ByteBuffer header = this.header.complete(size(tableChannel)).asCompletedByteBuffer();
        UncheckedIOUtils.write(tableChannel, header, 0); // doesn't update position in the channel
    }

    private void writeTable() {
        assert completedState == State.TEMPORAY_HEADER;
        assert position(tableChannel) == SSTableHeader.BUFFER_SIZE;

        memtable.applyInOrder((k, v) -> writeTableEntry((ByteBuffer) k, v));
    }

    /**
     * Table entry on disk format is :
     * 2 bytes for the row key size (n)
     * 4 bytes for the entry size (m)
     * n bytes for the key
     * m bytes for the value
     */
    protected void writeTableEntry(ByteBuffer key, Row value) {
        assert key.limit() <= Short.MAX_VALUE;
        assert key.position() == 0;

        SerializedRow serializedRow = Serializer.serialize(value);
        int totalSize = serializedRow.totalSize();

//        ByteBuffer keyValueLengthBuffer = ByteBuffer.allocate(2 + 4);
        ByteBuffer keyValueLengthBuffer = tableBuffer.getPreallocatedBuffer();
        assert keyValueLengthBuffer.position() == 0;
        assert keyValueLengthBuffer.limit() == TABLE_ENTRY_KEY_VALUE_LENGTH;
        keyValueLengthBuffer.putShort((short) key.limit());
        keyValueLengthBuffer.putInt(totalSize);

        UncheckedIOUtils.write(tableBuffer, (ByteBuffer) keyValueLengthBuffer.flip());
        // we have to duplicate because a reader may get a reference to the key while we are writing
        UncheckedIOUtils.write(tableBuffer, key.duplicate());
        UncheckedIOUtils.write(tableBuffer, serializedRow.getBuffers());

        valuesOffsets.add(offset);
        offset += keyValueLengthBuffer.limit() + key.limit() + totalSize;
        entriesSize.add(totalSize);

        bloomFilter.add(key);
    }

    private void writeIndex() {
        assert completedState == State.TABLE;
        Iterator<LongCursor> offsets = valuesOffsets.iterator();
        Iterator<IntCursor> sizes = entriesSize.iterator();
        memtable.applyInOrder((k, v) -> {
//            ByteBuffer keySizeOffsetAndValueSize = ByteBuffer.allocate(2 + 8 + 4);
            ByteBuffer keySizeOffsetAndValueSize = indexBuffer.getPreallocatedBuffer();
            assert keySizeOffsetAndValueSize.position() == 0;
            assert keySizeOffsetAndValueSize.limit() == INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH;
            assert offsets.hasNext();
            assert sizes.hasNext();
            long nextOffset = offsets.next().value;
            int nextEntrySize = sizes.next().value;
            writeIndexEntry(k, keySizeOffsetAndValueSize, nextOffset, nextEntrySize);
        });
    }

    /**
     * Index entry on disk format is :
     * 2 bytes for the row key size (n)
     * 8 bytes for the offset at which the target row starts in the table file
     * 4 bytes for the value size
     * n bytes for the row key
     */
    private void writeIndexEntry(ByteBuffer key, ByteBuffer keySizeOffsetAndValueSize, long nextOffset, int serializedValueSize) {
        assert key.limit() <= Short.MAX_VALUE;
        keySizeOffsetAndValueSize.clear();

        keySizeOffsetAndValueSize.putShort((short) key.limit())
                .putLong(nextOffset)
                .putInt(serializedValueSize)
                .flip();
        UncheckedIOUtils.write(indexBuffer, keySizeOffsetAndValueSize);
        UncheckedIOUtils.write(indexBuffer, (ByteBuffer) key.duplicate().position(0));
    }

    public void close() {
        // also closes the files
        UncheckedIOUtils.close(indexBuffer);
        UncheckedIOUtils.close(tableBuffer);
        completedState = State.CLOSED;
    }

    @Override
    protected void finalize() throws Throwable {
        if (completedState != State.CLOSED) {
            logger.warn("an SSTableWriter have not been correctly closed");
            close();
        }
        super.finalize();
    }

    protected enum State {
        NOTHING,
        TEMPORAY_HEADER,
        TABLE,
        INDEX,
        COMPLETE_HEADER,
        SYNCED,
        CLOSED
    }

    private static class BufferingChannel implements GatheringByteChannel, Flushable {

        public static final int BUFFERED_CHANNEL_BUFFER_SIZE = 256;

        private final FileChannel delegate;
        private final ByteBuffer[] buffer = new ByteBuffer[BUFFERED_CHANNEL_BUFFER_SIZE];
        private int bufferIndex = 0;
        private final BufferCache bufferCache;

        // this class is highly specific to the use case here because we have the buffering over
        // a channel but also a cache of preallocated buffers of a specific size, used here to
        // wirte the fixed-size part of the entries
        private BufferingChannel(FileChannel delegate, int preallocatedBufferSize) {
            this.delegate = delegate;
            this.bufferCache = new BufferCache(BUFFERED_CHANNEL_BUFFER_SIZE, preallocatedBufferSize);
        }

        public ByteBuffer getPreallocatedBuffer() {
            return bufferCache.get();
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
            if(bufferIndex == buffer.length) {
                flush();
            }
            return limit;
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        public void flush() throws IOException {
            if(bufferIndex != 0) {
                delegate.write(buffer, 0, bufferIndex);
                bufferIndex = 0; // no need to clear the array as the ByteBuffers are already referenced in the memtable, so no garbage is kept
                bufferCache.reset();
            }

        }
        @Override
        public void close() throws IOException {
            if(bufferIndex != 0) {
                throw new IllegalStateException("Write buffer have not been flushed before closing it");
            }
            delegate.close();
        }
    }

    @VisibleForTesting
    protected static class BufferCache {

        protected final ByteBuffer[] buffers;
        private int index = 0;

        BufferCache(int count, int size) {
            this.buffers = new ByteBuffer[count];
            ByteBuffer slab = ByteBuffer.allocateDirect(size * count);
            for (int i = 0; i < buffers.length; i++) {
                slab.position(i* size);
                buffers[i] = (ByteBuffer) slab.slice().limit(size);
            }
        }

        public ByteBuffer get() {
            assert index < buffers.length;
            return buffers[index++];
        }

        public void reset() {
            index = 0;
            for (ByteBuffer buffer : buffers) {
                buffer.position(0);
            }
        }
    }
}
