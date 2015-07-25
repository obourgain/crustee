package org.crustee.raft.storage.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableReaderTest {

    static final short KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    File table;
    File index;

    @Before
    public void create_sstable() {
        Memtable memtable = createMemtable(10);
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

        long expectedKeyOffset = SSTableHeader.BUFFER_SIZE + (2 + 4 + KEY_SIZE + VALUE_SIZE) + (2 + 4);
        long expectedValueOffset = expectedKeyOffset + KEY_SIZE;
        ByteBuffer key = ByteBuffer.allocate(KEY_SIZE).putInt(0, 1);
        KVLocalisation localisation = reader.findKVLocalisation(key);
        assertThat(localisation.getKeySize()).isEqualTo(KEY_SIZE);
        assertThat(localisation.getKeyOffset()).isEqualTo(expectedKeyOffset);
        assertThat(localisation.getValueOffset()).isEqualTo(expectedValueOffset);

        ByteBuffer value = reader.get(localisation);
        assertThat(value.limit()).isEqualTo(VALUE_SIZE);
        assertThat(value.position()).isEqualTo(0);
        assertThat(value).isEqualTo(ByteBuffer.allocate(VALUE_SIZE).putInt(0, 1));
    }

    private LockFreeBTree createMemtable(int entries) {
        LockFreeBTree bTree = new LockFreeBTree(16);
        IntStream.range(0, entries).forEach(i -> bTree.insert(ByteBuffer.allocate(KEY_SIZE).putInt(0, i), ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)));
        return bTree;
    }
}