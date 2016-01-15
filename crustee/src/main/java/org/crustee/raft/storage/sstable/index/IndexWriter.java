package org.crustee.raft.storage.sstable.index;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.sstable.BufferingChannel;
import org.crustee.raft.utils.UncheckedIOUtils;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.LongCursor;

public class IndexWriter {

    public static final int INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH = Short.BYTES + Long.BYTES + Integer.BYTES;

    private final ReadOnlyMemtable memtable;
    private final LongArrayList valuesOffsets;
    private final IntArrayList entriesSize;
    private final BufferingChannel indexChannel;

    public IndexWriter(ReadOnlyMemtable memtable, LongArrayList valuesOffsets, IntArrayList entriesSize, BufferingChannel indexChannel) {
        this.memtable = memtable;
        this.valuesOffsets = valuesOffsets;
        this.entriesSize = entriesSize;
        this.indexChannel = indexChannel;
    }

    public void write() {
        Iterator<LongCursor> offsets = valuesOffsets.iterator();
        Iterator<IntCursor> sizes = entriesSize.iterator();
        memtable.applyInOrder((k, v) -> {
            ByteBuffer keySizeOffsetAndValueSize = indexChannel.getPreallocatedBuffer();
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
    @VisibleForTesting
    void writeIndexEntry(ByteBuffer key, ByteBuffer keySizeOffsetAndValueSize, long nextOffset, int serializedValueSize) {
        assert key.limit() <= Short.MAX_VALUE;
        keySizeOffsetAndValueSize.clear();

        keySizeOffsetAndValueSize.putShort((short) key.limit())
                .putLong(nextOffset)
                .putInt(serializedValueSize)
                .flip();

        UncheckedIOUtils.write(indexChannel, keySizeOffsetAndValueSize);
        UncheckedIOUtils.write(indexChannel, (ByteBuffer) key.duplicate().position(0));
    }

}
