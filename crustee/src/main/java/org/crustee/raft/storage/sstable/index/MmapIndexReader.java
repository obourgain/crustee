package org.crustee.raft.storage.sstable.index;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import org.crustee.raft.storage.sstable.RowLocation;
import org.crustee.raft.utils.ByteBufferUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class MmapIndexReader implements IndexReader {

    private static final Logger logger = getLogger(MmapIndexReader.class);

    private final MappedByteBuffer map;
    private final long indexFileSize;
    private volatile boolean closed = false;

    public MmapIndexReader(Path index) {
        map = UncheckedIOUtils.mapReadOnly(index);
        indexFileSize = map.limit();
    }

    /**
     * Find the offset where the value of the searched key is located in the SSTable file, or -1
     */
    public RowLocation findRowLocation(ByteBuffer searchedKey) {
        assert searchedKey.limit() <= Short.MAX_VALUE : "key may not be longer than " + Short.MAX_VALUE + " bytes";

        short searchedKeySize = (short) searchedKey.limit();
        int sizeOf_KeySize_Offset_ValueSize = 2 + 8 + 4;

        int position = 0;
        // TODO what if the file is corrupted and have wrong size ?
        while (position < indexFileSize) {
            short keySize = map.getShort(position);
            if (searchedKeySize != keySize) {
                position += sizeOf_KeySize_Offset_ValueSize;
                position += keySize;
                // the key size is not the same, don't even bother to compare those, go to next entry
                continue;
            }
            position += 2; // keySize
            long offset = map.getLong(position);
            position += 8; // offset
            int valueSize = map.getInt(position);
            position += 4; // key

            if (ByteBufferUtils.equals(searchedKey, map, position, position + keySize)) {
                return new RowLocation(keySize, offset, valueSize);
            }
            position += keySize;
        }
        return RowLocation.NOT_FOUND;
    }

    @Override
    public void close() throws IOException {
        boolean unmapped = ByteBufferUtils.tryUnmap(map);
        if (!unmapped) {
            logger.info("memory mapped index was not closed, this may cause GC later");
        }
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            logger.warn("Index was not closed before finalization");
            close();
        }
    }

}
