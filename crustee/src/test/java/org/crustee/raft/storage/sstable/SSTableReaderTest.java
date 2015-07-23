package org.crustee.raft.storage.sstable;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.crustee.raft.storage.row.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableReaderTest {

    static final short ROW_KEY_SIZE = 32;
    static final short COLUMN_KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    File table;
    File index;

    @Before
    public void create_sstable() {
        Memtable memtable = createMemtable(3);
        try {
            table = temporaryFolder.newFile();
            index = temporaryFolder.newFile();
            try (SSTableWriter writer = new SSTableWriter(table.toPath(), index.toPath(), memtable)) {
                writer.write();
                new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void should_find_value_offset() throws IOException {
        SSTableReader reader = new SSTableReader(table.toPath(), index.toPath());

        long expectedRowKeyOffset =
                SSTableHeader.BUFFER_SIZE +
                        // first entry with kv size, rowkey, row (kv size + columnkey + value)
                        (Short.BYTES + Integer.BYTES + // row key & value size
                                ROW_KEY_SIZE +
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
        // TODO deserialize value to check its state
//        assertThat(value.asMap()).isEqualTo(VALUE_SIZE);
//        assertThat(value.position()).isEqualTo(0);
//        assertThat(value).isEqualTo(ByteBuffer.allocate(VALUE_SIZE).putInt(0, 1));
    }

    private LockFreeBTree createMemtable(int entries) {
        LockFreeBTree bTree = new LockFreeBTree(16);
        IntStream.range(0, entries).forEach(i -> bTree.insert(ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, i),
                        singletonMap(
                                ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, i),
                                ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)))
        );
        return bTree;
    }
}