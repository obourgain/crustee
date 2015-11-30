package org.crustee.raft.storage.sstable.index;

import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.function.BiPredicate;
import java.util.function.ObjIntConsumer;
import org.crustee.raft.storage.sstable.RowLocation;
import org.crustee.raft.utils.ByteBufferUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class MmapIndexReader implements InternalIndexReader {

    private static final Logger logger = getLogger(MmapIndexReader.class);

    private static final int SIZEOF_KEYSIZE_OFFSET_VALUESIZE = 2 + 8 + 4;

    private final MappedByteBuffer map;
    private final long indexFileSize;
    private volatile boolean closed = false;

    public MmapIndexReader(Path index) {
        map = UncheckedIOUtils.map(index);
        indexFileSize = map.limit();
    }

    public static long globalScannedEntries = 0;

    /**
     * Find the offset where the value of the searched key is located in the SSTable file, or -1
     */
    // TODO test these params
    public RowLocation findRowLocation(ByteBuffer searchedKey, int startAt, int maxScannedEntry) {
        assert searchedKey.limit() <= Short.MAX_VALUE : "key may not be longer than " + Short.MAX_VALUE + " bytes";
        assert startAt <= indexFileSize : "start searching at " + startAt + " but index file size is " + indexFileSize;

        short searchedKeySize = (short) searchedKey.limit();

        int position = startAt;
        int entriesScanned = 0;
        // TODO what if the file is corrupted and have wrong size ?
        while (position < indexFileSize && entriesScanned < maxScannedEntry) {
            globalScannedEntries++;
            short keySize = map.getShort(position);
            assert keySize >= 0;
            if (searchedKeySize != keySize) {
                position += SIZEOF_KEYSIZE_OFFSET_VALUESIZE;
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
            entriesScanned++;
        }
        return RowLocation.NOT_FOUND;
    }

    /**
     * Traverse this index file and for each entry apply the given filter. If the entry matches the filter, the callback
     * is called with the index value.
     * The filter will be provided with a copy of the buffer. It must not attempt to modify it. If it modifies the {@link ByteBuffer#position}
     * or other {@link ByteBuffer} fields, it will receive the same ByteBuffer in the callback
     */
    // TODO avoid allocating so many BB, and the boxing, it should basically be allocation-less if no entry match
    public void iterate(BiPredicate<ByteBuffer, Integer> keyAndEntryIndexFilter, ObjIntConsumer<ByteBuffer> callback) {
//        Cursor cursor = new Cursor();
//
//        while (cursor.hasNext()) {
//            cursor.moveToNext();
//            if(cursor.accept(keyAndEntryIndexFilter)) {
//                callback.accept(cursor.currentEntryKeyBuffer(), cursor.position);
//            }
//        }

        int indexEntry = 0;
        int position = 0;
        // TODO what if the file is corrupted and have wrong size ?
        while (position < indexFileSize) {
            int tempPosition = position;
            short keySize = map.getShort(position);
            tempPosition += 2; // keySize
//            long offset = map.getLong(position);
            tempPosition += 8; // offset
//            int valueSize = map.getInt(position);
            tempPosition += 4; // key

            ByteBuffer dup = map.duplicate();
            dup.position(tempPosition).limit(tempPosition + keySize);
            ByteBuffer slice = dup.slice();

            if (keyAndEntryIndexFilter.test(slice, indexEntry)) {
                callback.accept(slice, position);
            }
            position = tempPosition + keySize;
            indexEntry++;
        }
    }

    private class Cursor {
        // position in bytes
        private int position = 0;
        // position in entries count
        private int index = 0;
        private ByteBuffer current = null;

        private ByteBuffer currentEntryKeyBuffer() {
            if(current == null) {
                ByteBuffer dup = map.duplicate();
                dup.position(position).limit(position + currentKeySize());
                current = dup.slice();
            }
            return current;
        }

        private int currentEntrySize() {
            // total size
            return SIZEOF_KEYSIZE_OFFSET_VALUESIZE + currentKeySize();
        }

        private short currentKeySize() {
            assert hasNext();
            return map.getShort(position);
        }

        private boolean hasNext() {
            return position < MmapIndexReader.this.indexFileSize;
        }

        private void moveToNext() {
            index++;
            position += currentEntrySize();
            current = null;
        }

        public boolean accept(BiPredicate<ByteBuffer, Integer> keyAndEntryIndexFilter) {
            return keyAndEntryIndexFilter.test(currentEntryKeyBuffer(), index);
        }
    }

    @Override
    public void close() throws IOException {
        boolean unmapped = ByteBufferUtils.tryUnmap(map);
        if (!unmapped) {
            logger.info("memory mapped index memory was not released, this may cause GC later");
        }
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            logger.warn("Index was not closed before finalization");
            close();
        }
        super.finalize();
    }

}
