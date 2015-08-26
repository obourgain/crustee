package org.crustee.raft.storage.sstable;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.row.Row;
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

                    SSTableReader reader = new SSTableReader(table.toPath(), index.toPath());

                    ByteBuffer key = ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, 1);
                    Optional<Row> value = reader.get(key);
                    Assertions.assertThat(value).isPresent();
                    assertThat(value.get().asMap()).isEqualTo(singletonMap(
                            ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, 1),
                            ByteBuffer.allocate(VALUE_SIZE).putInt(0, 1)));
                });
    }

    private WritableMemtable createMemtable(int entries) {
        WritableMemtable memtable = new LockFreeBTreeMemtable();
        IntStream.range(0, entries).forEach(i -> memtable.insert(ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, i),
                        singletonMap(
                                ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, i),
                                ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)))
        );
        return memtable;
    }
}
