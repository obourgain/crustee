package org.crustee.raft.storage.sstable;

import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.crustee.raft.storage.bloomfilter.ReadOnlyBloomFilter;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.index.IndexReader;
import org.crustee.raft.storage.sstable.index.IndexReaderFactory;
import org.crustee.raft.storage.table.Timestamped;
import org.crustee.raft.utils.CloseableUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class SSTableReader implements AutoCloseable, Timestamped {

    private static final Logger logger = getLogger(SSTableReader.class);

    private final FileChannel tableChannel;
    private final IndexReader indexReader;
    private final ReadOnlyBloomFilter bloomFilter;

    protected SSTableHeader header;
    private volatile boolean closed;

    // counts how many time get() was called
    private LongAdder getCounter = new LongAdder();
    // counts how many time a read was done, so when the bloom filter didn't filter out this table
    private LongAdder effectiveGetCounter = new LongAdder();

    public SSTableReader(Path table, Path index, ReadOnlyBloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
        this.tableChannel = openChannel(table, StandardOpenOption.READ);

        this.indexReader = IndexReaderFactory.create(index);

        ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.BUFFER_SIZE);
        UncheckedIOUtils.read(tableChannel, buffer);
        this.header = SSTableHeader.fromBuffer((ByteBuffer) buffer.flip());
    }

    public void close() {
        // also closes the files
        CloseableUtils.close(indexReader);
        CloseableUtils.close(tableChannel);
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        if (closed) {
            logger.warn("an SSTableWriter have not been closed, this may cause a resource leak");
            close();
        }
        super.finalize();
    }

    public Optional<Row> get(ByteBuffer key) {
        getCounter.increment();
        if(bloomFilter.mayBePresent(key)) {
            effectiveGetCounter.increment();
            RowLocation rowLocation = indexReader.findRowLocation(key);
            if(rowLocation.isFound()) {
                ByteBuffer buffer = ByteBuffer.allocate(rowLocation.getValueSize());
                UncheckedIOUtils.read(tableChannel, buffer, rowLocation.getValueOffset());
                buffer.flip(); // ready to read
                return Optional.of(Serializer.deserialize(new SerializedRow(buffer)));
            }
        }
        return Optional.empty();
    }

    public long getCount() {
        return getCounter.longValue();
    }

    public long getReadCount() {
        return effectiveGetCounter.longValue();
    }

    @Override
    public long getCreationTimestamp() {
        return header.getCreationTimestamp();
    }
}
