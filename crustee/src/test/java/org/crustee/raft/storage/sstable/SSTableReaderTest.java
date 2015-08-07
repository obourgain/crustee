package org.crustee.raft.storage.sstable;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.Memtable;
import org.crustee.raft.storage.row.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableReaderTest extends AbstractSSTableTest {

    static final short ROW_KEY_SIZE = 32;
    static final short COLUMN_KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    static void init_sstable(SSTableWriter writer, File table, File index) {
        writer.write();
        new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
    }

    @Test
    public void should_find_value_offset() throws IOException {
        Memtable memtable = createMemtable(3);

        test(memtable, SSTableReaderTest::init_sstable,
	        (writer, table, index) -> {

            SSTableReader reader = new SSTableReader(table.toPath(), index.toPath());

            long expectedRowKeyOffset =
                    SSTableHeader.BUFFER_SIZE +
                            // first entry with kv size, rowkey, row (kv size + columnkey + value)
                            (Short.BYTES + Integer.BYTES + // row key & value size
                                    ROW_KEY_SIZE +
                                    Integer.BYTES + // number of columns
                                    Short.BYTES + Integer.BYTES + COLUMN_KEY_SIZE + VALUE_SIZE) + // column key size + value size
                            // second entry with kv size
                            (Short.BYTES + Integer.BYTES) // second row key & value size
                    ;

            long expectedValueOffset = expectedRowKeyOffset + ROW_KEY_SIZE;
            ByteBuffer key = ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, 1);
            KVLocation location = reader.findKVLocalisation(key);
            assertThat(location.getRowKeySize()).isEqualTo(ROW_KEY_SIZE);
            assertThat(location.getRowKeyOffset()).isEqualTo(expectedRowKeyOffset);
            assertThat(location.getValueOffset()).isEqualTo(expectedValueOffset);

            Row value = reader.get(location);
            assertThat(value.asMap()).isEqualTo(singletonMap(
                    ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, 1),
                    ByteBuffer.allocate(VALUE_SIZE).putInt(0, 1)));
        });
    }

    private Memtable createMemtable(int entries) {
        Memtable memtable = new LockFreeBTreeMemtable();
        IntStream.range(0, entries).forEach(i -> memtable.insert(ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, i),
                        singletonMap(
                                ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, i),
                                ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)))
        );
        return memtable;
    }
}
