package org.crustee.raft.storage.sstable;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.row.Row;
import org.junit.Assert;
import org.junit.Test;

public class SSTableReaderTest extends AbstractSSTableTest {

    static final short ROW_KEY_SIZE = 32;
    static final short COLUMN_KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    @Test
    public void should_find_value_offset() throws IOException {
        WritableMemtable memtable = createMemtable(3);

        test(memtable, this::initSstable,
                (writer, table, index) -> {

                    try (SSTableReader reader = new SSTableReader(table.toPath(), index.toPath(), key -> true)) {

                        ByteBuffer key = ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, 1);
                        Row value = reader.get(key);
                        Assert.assertNotNull(value);
                        assertThat(value.asMap()).isEqualTo(singletonMap(
                                ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, 1),
                                ByteBuffer.allocate(VALUE_SIZE).putInt(0, 1)));

                        assertThat(reader.getCount()).isEqualTo(1);
                        assertThat(reader.getReadCount()).isEqualTo(1);
                    }
                });
    }

    @Test
    public void should_skip_reading_not_in_bloom_filter() throws IOException {
        WritableMemtable memtable = createMemtable(3);

        test(memtable, this::initSstable,
                (writer, table, index) -> {
                    try (SSTableReader reader = new SSTableReader(table.toPath(), index.toPath(), key -> false)) {
                        ByteBuffer key = ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, 42);
                        Row value = reader.get(key);
                        Assert.assertNull(value);
                        assertThat(reader.getCount()).isEqualTo(1);
                        assertThat(reader.getReadCount()).isEqualTo(0);
                    }
                });
    }

    private WritableMemtable createMemtable(int entries) {
        WritableMemtable memtable = new LockFreeBTreeMemtable(1L);
        IntStream.range(0, entries).forEach(i -> memtable.insert(ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, i),
                singletonMap(
                        ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, i),
                        ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)))
        );
        return memtable;
    }
}
