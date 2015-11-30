package org.crustee.raft.storage.sstable.index;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.sstable.BufferingChannel;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;

public class IndexWriterTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    BufferingChannel indexChannel;

    private WritableMemtable memtable = new LockFreeBTreeMemtable(System.currentTimeMillis());

    @Test
    public void should_call_write_twice_per_entry() throws IOException {
        when(indexChannel.getPreallocatedBuffer()).thenReturn(ByteBuffer.allocate(IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH));
        int entryCount = 6;

        for (int i = 0; i < entryCount; i++) {
            memtable.insert(ByteBuffer.allocate(4).putInt(0, i), Collections.singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(12)));
        }

        LongArrayList valuesOffsets = new LongArrayList(entryCount);
        for (int i = 0; i < entryCount; i++) {
            valuesOffsets.add(i * 16);
        }
        IntArrayList entriesSize = new IntArrayList(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entriesSize.add(i * 32);
        }

        IndexWriter indexWriter = new IndexWriter(memtable, valuesOffsets, entriesSize, indexChannel);

        indexWriter.write();

        verify(indexChannel, times(entryCount * 2)).write((ByteBuffer) any());
    }

    @Test
    public void should_create_correct_entries() throws Exception {

        ByteBuffer rowKey = ByteBuffer.allocate(4).putInt(0, 12);
        memtable.insert(rowKey, Collections.singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(12)));

        LongArrayList valuesOffsets = new LongArrayList();
        int offset = 42;
        valuesOffsets.add(offset);
        IntArrayList entriesSize = new IntArrayList();
        int entrySize = 52;
        entriesSize.add(entrySize);

        IndexWriter indexWriter = new IndexWriter(memtable, valuesOffsets, entriesSize, indexChannel);

        indexWriter.writeIndexEntry(rowKey, ByteBuffer.allocate(IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH), offset, entrySize);

        ByteBuffer expectedMetadata = (ByteBuffer) ByteBuffer.allocate(IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH)
                .putShort((short) rowKey.limit())
                .putLong(offset)
                .putInt(entrySize)
                .flip();

        verify(indexChannel).write(expectedMetadata);
        verify(indexChannel).write(rowKey);
    }
}
