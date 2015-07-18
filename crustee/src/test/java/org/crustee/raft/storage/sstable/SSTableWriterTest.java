package org.crustee.raft.storage.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.storage.sstable.SSTableWriter.State.INDEX;
import static org.crustee.raft.utils.UncheckedIOUtils.readAllToBuffer;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableWriterTest {

    static final int KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("/home/olivier/temp"));

    @Test
    public void should_write_header() throws Exception {
        Memtable memtable = createMemtable(10);

        try {
            File table = temporaryFolder.newFile();
            File index = temporaryFolder.newFile();
            try (SSTableWriter writer = new SSTableWriter(table.toPath(), index.toPath(), memtable)) {
                writer.writeTemporaryHeader();

                SSTableHeader tempHeader = SSTableHeader.fromBuffer(readAllToBuffer(table.toPath()));
                // don't use isEqualTo to avoid comparing the completed field
                assertThat(tempHeader.getSize()).isEqualTo(writer.header.getSize());
                assertThat(tempHeader.getEntryCount()).isEqualTo(writer.header.getEntryCount());

                writer.completedState = INDEX;
                writer.writeCompletedHeader();

                long expectedTableSize = SSTableHeader.BUFFER_SIZE;
                long tableFileSize = table.length();
                assertThat(tableFileSize).isEqualTo(expectedTableSize);

                SSTableHeader header = SSTableHeader.fromBuffer(readAllToBuffer(table.toPath()));
                assertThat(header).isEqualTo(writer.header);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void should_write() throws Exception {
        int entries = 100_000;
        Memtable memtable = createMemtable(entries);

        try {
            File table = temporaryFolder.newFile();
            File index = temporaryFolder.newFile();
            try (SSTableWriter writer = new SSTableWriter(table.toPath(), index.toPath(), memtable)) {
                writer.write();

                long expectedTableSize = entries * (KEY_SIZE + VALUE_SIZE + 2 + 4) + SSTableHeader.BUFFER_SIZE; // key size (short + value size (int)
                long tableFileSize = table.length();
                assertThat(tableFileSize).isEqualTo(expectedTableSize);

                long expectedIndexSize = entries * (KEY_SIZE + 2 + 8 + 4); // the 8 is for the offset stored in the index, 2 is for key size + 4 for value size
                long indexFileSize = index.length();
                assertThat(indexFileSize).isEqualTo(expectedIndexSize);

                new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Ignore("convert that into a proper jmh bench")
    public void diry_perf_test() throws Exception {
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            int entries = 100_000;
            Memtable memtable = createMemtable(entries);

            try {
                File table = temporaryFolder.newFile();
                File index = temporaryFolder.newFile();
                try (SSTableWriter writer = new SSTableWriter(table.toPath(), index.toPath(), memtable)) {
                    writer.write();

                    long expectedTableSize = entries * (KEY_SIZE + VALUE_SIZE + 2 + 2) + SSTableHeader.BUFFER_SIZE; // the 2s are for key and value size  as shorts
                    long tableFileSize = table.length();
                    assertThat(tableFileSize).isEqualTo(expectedTableSize);

                    long expectedIndexSize = entries * (KEY_SIZE + 8 + 2); // the 8 is for the offset stored in the index, 2 is for key size
                    long indexFileSize = index.length();
                    assertThat(indexFileSize).isEqualTo(expectedIndexSize);

                    new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            long end = System.currentTimeMillis();
            System.out.println("duration " + (end - start));
        }
    }

    private LockFreeBTree createMemtable(int entries) {
        LockFreeBTree bTree = new LockFreeBTree(16);
        IntStream.range(0, entries).forEach(i -> bTree.insert(ByteBuffer.allocate(KEY_SIZE).putInt(0, i), ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)));
        return bTree;
    }

}