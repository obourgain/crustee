package org.crustee.raft.storage.sstable;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.storage.sstable.SSTableWriter.State.INDEX;
import static org.crustee.raft.utils.UncheckedIOUtils.readAllToBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableWriterTest extends AbstractSSTableTest {

    static final int ROW_KEY_SIZE = 32;
    static final int COLUMN_KEY_SIZE = 16;
    static final int VALUE_SIZE = 100;

    @Test
    public void should_write_header() throws IOException {
        WritableMemtable memtable = createMemtable(10);

        test(memtable, (writer, table, index) -> {
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
        });
    }

    @Test
    public void should_write() throws IOException {
        int entries = 100_000;
        WritableMemtable memtable = createMemtable(entries);

        test(memtable, (writer, table, index) -> {
            writer.write();

            long expectedTableSize = entries * (Short.BYTES + Integer.BYTES + ROW_KEY_SIZE + // row key & value size
                    Integer.BYTES + // number of columns in the row
                    Short.BYTES + Integer.BYTES + COLUMN_KEY_SIZE + VALUE_SIZE) + // column key size + value size
                    SSTableHeader.BUFFER_SIZE;
            long tableFileSize = table.length();
            assertThat(tableFileSize).isEqualTo(expectedTableSize);

            long expectedIndexSize = entries * (ROW_KEY_SIZE + 2 + 8 + 4); // the 8 is for the offset stored in the index, 2 is for key size + 4 for value size
            long indexFileSize = index.length();
            assertThat(indexFileSize).isEqualTo(expectedIndexSize);

            new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
        });
    }

    @Test
    @Ignore("convert that into a proper jmh bench")
    public void diry_perf_test() throws IOException {
        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();
            int entries = 1000_000;
            WritableMemtable memtable = createMemtable(entries);

            test(memtable, (writer, tbl, idx) -> {
                Path table = tbl.toPath();
                Path index = idx.toPath();

                writer.write();

                long expectedTableSize = entries * (Short.BYTES + Integer.BYTES + ROW_KEY_SIZE + // row key & value size
                        Short.BYTES + Integer.BYTES + COLUMN_KEY_SIZE + VALUE_SIZE) + // column key size + value size
                        SSTableHeader.BUFFER_SIZE;
                long tableFileSize = Files.size(table);
                assertThat(tableFileSize).isEqualTo(expectedTableSize);

                long expectedIndexSize = entries * (ROW_KEY_SIZE + 2 + 8 + 4); // the 8 is for the offset stored in the index, 2 is for key size + 4 for value size
                long indexFileSize = Files.size(index);
                assertThat(indexFileSize).isEqualTo(expectedIndexSize);

                new SSTableConsistencyChecker(table, index, Assert::fail).check();
            });

            long end = System.currentTimeMillis();
            System.out.println("duration " + (end - start));
        }
    }

    private WritableMemtable createMemtable(int entries) {
        WritableMemtable memtable = new LockFreeBTreeMemtable();
        IntStream.range(0, entries).forEach(i -> memtable.insert(ByteBuffer.allocate(ROW_KEY_SIZE).putInt(0, i).putInt(4, i),
                        singletonMap(
                                ByteBuffer.allocate(COLUMN_KEY_SIZE).putInt(0, i),
                                ByteBuffer.allocate(VALUE_SIZE).putInt(0, i)))
        );
        return memtable;
    }

}
