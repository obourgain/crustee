package org.crustee.raft.storage.sstable;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.crustee.raft.utils.UncheckedIOUtils.fsync;
import static org.crustee.raft.utils.UncheckedIOUtils.fsyncDir;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.position;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import org.crustee.raft.storage.bloomfilter.BloomFilter;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.index.IndexWriter;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;

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

    SSTableHeader header;
    State completedState = State.NOTHING;

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

        // no need to close the index writer, we close its resources when closing the SSTableWriter
        IndexWriter indexWriter = new IndexWriter(memtable, valuesOffsets, entriesSize, indexBuffer);
        indexWriter.write();
        completedState = State.INDEX;

        // We flush right now because if a sstable reader is created from this writer and exposed to
        // reader threads, the flush may not have yet occurred. Furthermore, we need to
        // flush before fsync'ing.
        UncheckedIOUtils.flush(tableBuffer);
        UncheckedIOUtils.flush(indexBuffer);

        writeCompletedHeader();
        completedState = State.COMPLETE_HEADER;

        // put the bloom filter next to the index, if it is moved in an other directory, this dir will need to be fsync'ed separately
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

    void writeTemporaryHeader() {
        assert completedState == State.NOTHING;
        ByteBuffer header = this.header.asTemporaryByteBuffer();
        UncheckedIOUtils.write(tableChannel, header); // updates position in the channel
        offset += SSTableHeader.BUFFER_SIZE;
    }

    void writeCompletedHeader() {
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
    void writeTableEntry(ByteBuffer key, Row value) {
        assert key.limit() <= Short.MAX_VALUE;
        assert key.position() == 0;

        SerializedRow serializedRow = Serializer.serialize(value);
        int totalSize = serializedRow.totalSize();

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

    enum State {
        NOTHING,
        TEMPORAY_HEADER,
        TABLE,
        INDEX,
        COMPLETE_HEADER,
        SYNCED,
        CLOSED
    }

}
