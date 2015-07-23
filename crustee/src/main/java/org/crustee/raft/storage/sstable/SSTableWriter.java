package org.crustee.raft.storage.sstable;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.crustee.raft.utils.UncheckedIOUtils.fsync;
import static org.crustee.raft.utils.UncheckedIOUtils.fsyncDir;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.position;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.LongCursor;

public class SSTableWriter implements AutoCloseable {

    private static final Logger logger = getLogger(SSTableWriter.class);

    private final Path table;
    private final Path index;
    private final Memtable memtable;

    private final FileChannel indexChannel;
    private final FileChannel tableChannel;

    private final LongArrayList valuesOffsets;
    private final IntArrayList entriesSize;

    protected SSTableHeader header;
    protected State completedState = State.NOTHING;

    private long offset = 0;

    public SSTableWriter(Path table, Path index, Memtable memtable) {
        this.table = table;
        this.index = index;
        this.memtable = memtable;
        this.valuesOffsets = new LongArrayList(memtable.getCount());
        this.entriesSize = new IntArrayList(memtable.getCount());

        this.tableChannel = openChannel(table, WRITE);
        this.indexChannel = openChannel(index, WRITE);

        this.header = new SSTableHeader(memtable.getCount());
    }

    public void write() {
        writeTemporaryHeader();
        completedState = State.TEMPORAY_HEADER;
        writeTable();
        completedState = State.TABLE;
        writeIndex();
        completedState = State.INDEX;
        writeCompletedHeader();
        completedState = State.COMPLETE_HEADER;

        fsync(tableChannel, true);
        fsync(indexChannel, true);
        Path tableDir = table.getParent();
        Path indexDir = index.getParent();
        fsyncDir(tableDir);
        if(!indexDir.equals(tableDir)) {
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

    private void writeTableEntry(ByteBuffer key, Row value) {
        assert key.limit() <= Short.MAX_VALUE;
        assert key.position() == 0;

        SerializedRow serializedRow = Serializer.serialize(value);
        int totalSize = serializedRow.totalSize();

        ByteBuffer keyValueLengthBuffer = ByteBuffer.allocate(2 + 4);
        keyValueLengthBuffer.putShort((short) key.limit());
        keyValueLengthBuffer.putInt(totalSize);

        ByteBuffer[] buffers = concat(
                (ByteBuffer) keyValueLengthBuffer.flip(),
                // we have to duplicate because a reader may get a reference to the key while we are writing
                key.duplicate(),
                serializedRow.getBuffers()
        );

        UncheckedIOUtils.write(tableChannel, buffers);

        offset += keyValueLengthBuffer.limit() + key.limit();
        valuesOffsets.add(offset);
        entriesSize.add(totalSize);
        offset += totalSize;
    }

    private ByteBuffer[] concat(ByteBuffer keyValueLengthBuffer, ByteBuffer rowKey, ByteBuffer[] buffers) {
        ByteBuffer[] result = new ByteBuffer[1 + 1 + buffers.length];
        result[0] = keyValueLengthBuffer;
        result[1] = rowKey;
        System.arraycopy(buffers, 0, result, 2, buffers.length);
        return result;
    }

    private void writeIndex() {
        assert completedState == State.TABLE;
        ByteBuffer keySizeOffsetAndValueSize = ByteBuffer.allocate(2 + 8 + 4);
        Iterator<LongCursor> offsets = valuesOffsets.iterator();
        Iterator<IntCursor> sizes = entriesSize.iterator();
        memtable.applyInOrder((k, v) -> {
            assert offsets.hasNext();
            assert sizes.hasNext();
            long nextOffset = offsets.next().value;
            int nextEntrySize = sizes.next().value;
            writeIndexEntry((ByteBuffer) k, keySizeOffsetAndValueSize, nextOffset, nextEntrySize);
        });
    }

    private void writeIndexEntry(ByteBuffer key, ByteBuffer keySizeOffsetAndValueSize, long nextOffset, int serializedValueSize) {
        assert key.limit() <= Short.MAX_VALUE;
        keySizeOffsetAndValueSize.clear();

        keySizeOffsetAndValueSize.putShort((short) key.limit())
                .putLong(nextOffset)
                .putInt(serializedValueSize)
                .flip();
        UncheckedIOUtils.write(indexChannel, new ByteBuffer[]{
                keySizeOffsetAndValueSize,
                (ByteBuffer) key.duplicate().position(0)
        });
    }

    public void close() {
        // also closes the files
        UncheckedIOUtils.close(indexChannel);
        UncheckedIOUtils.close(tableChannel);
        completedState = State.CLOSED;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (completedState != State.CLOSED) {
            logger.warn("an SSTableWriter have not been correctly closed");
            close();
        }
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
}
