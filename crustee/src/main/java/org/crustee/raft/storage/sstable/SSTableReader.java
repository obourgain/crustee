package org.crustee.raft.storage.sstable;

import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class SSTableReader implements AutoCloseable {

    private static final Logger logger = getLogger(SSTableReader.class);

    private final FileChannel indexChannel;
    private final long indexFileSize;
    private final FileChannel tableChannel;

    protected SSTableHeader header;
    private volatile boolean closed;

    public SSTableReader(Path table, Path index) {
        this.tableChannel = openChannel(table, StandardOpenOption.READ);
        this.indexChannel = openChannel(index, StandardOpenOption.READ);
        this.indexFileSize = size(indexChannel);

        ByteBuffer buffer = ByteBuffer.allocate(SSTableHeader.BUFFER_SIZE);
        UncheckedIOUtils.read(tableChannel, buffer);
        this.header = SSTableHeader.fromBuffer(buffer);
    }

    /**
     * Find the offset where the value of the searched key is located in the SSTable file, or -1
     */
    public KVLocation findKVLocalisation(ByteBuffer searchedKey) {
        assert searchedKey.limit() >= Short.SIZE : "key may not be longer than " + Short.MAX_VALUE + " bytes";

        short searchedKeySize = (short) searchedKey.limit();
        ByteBuffer keyBuffer = ByteBuffer.allocate(searchedKeySize);
        ByteBuffer keySizeOffsetValueSize = ByteBuffer.allocate(2 + 8 + 4);

        long position = 0;
        // TODO what if the file is corrupted and have wrong size ?
        while (position < indexFileSize) {
            keySizeOffsetValueSize.clear();
            UncheckedIOUtils.read(indexChannel, keySizeOffsetValueSize, position);
            position += keySizeOffsetValueSize.capacity();
            keySizeOffsetValueSize.flip();
            // TODO we could read the size and offset of next K here while reading the current K to save an io
            short keySize = keySizeOffsetValueSize.getShort();
            if (searchedKeySize != keySize) {
                position += keySize;
                // the key size is not the same, don't even bother to compare those, go to next entry
                continue;
            }

            keyBuffer.clear();
            UncheckedIOUtils.read(indexChannel, keyBuffer, position);
            position += keySize;
            keyBuffer.flip();
            if (searchedKey.equals(keyBuffer)) {
                long offset = keySizeOffsetValueSize.getLong();
                int valueSize = keySizeOffsetValueSize.getInt();
                return new KVLocation(keySize, offset, valueSize);
            }
        }
        return KVLocation.NOT_FOUND;
    }

    public void close() {
        // also closes the files
        UncheckedIOUtils.close(indexChannel);
        UncheckedIOUtils.close(tableChannel);
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

    public Row get(KVLocation location) {
        assert location.isFound();
        ByteBuffer buffer = ByteBuffer.allocate(location.getValueSize());
        UncheckedIOUtils.read(tableChannel, buffer, location.getValueOffset());
        buffer.flip(); // ready to read
        return Serializer.deserialize(new SerializedRow(buffer));
    }
}
