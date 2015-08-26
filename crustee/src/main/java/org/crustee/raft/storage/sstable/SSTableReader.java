package org.crustee.raft.storage.sstable;

import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.index.IndexReader;
import org.crustee.raft.storage.sstable.index.IndexReaderFactory;
import org.crustee.raft.utils.CloseableUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class SSTableReader implements AutoCloseable {

    private static final Logger logger = getLogger(SSTableReader.class);

    private final FileChannel tableChannel;
    private final IndexReader indexReader;

    protected SSTableHeader header;
    private volatile boolean closed;

    public SSTableReader(Path table, Path index) {
        this.tableChannel = openChannel(table, StandardOpenOption.READ);

        this.indexReader = IndexReaderFactory.create(index);

        ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.BUFFER_SIZE);
        UncheckedIOUtils.read(tableChannel, buffer);
        this.header = SSTableHeader.fromBuffer(buffer);
    }

    public void close() {
        // also closes the files
        CloseableUtils.close(indexReader);
        CloseableUtils.close(tableChannel);
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (closed) {
            logger.warn("an SSTableWriter have not been closed, this may cause a resource leak");
            close();
        }
    }

    public Optional<Row> get(ByteBuffer key) {
        RowLocation rowLocation = indexReader.findRowLocation(key);
        if(rowLocation.isFound()) {
            ByteBuffer buffer = ByteBuffer.allocate(rowLocation.getValueSize());
            UncheckedIOUtils.read(tableChannel, buffer, rowLocation.getValueOffset());
            buffer.flip(); // ready to read
            return Optional.of(Serializer.deserialize(new SerializedRow(buffer)));
        } else {
            return Optional.empty();
        }
    }
}
