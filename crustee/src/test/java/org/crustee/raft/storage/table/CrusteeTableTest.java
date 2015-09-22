package org.crustee.raft.storage.table;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.SortedMap;
import java.util.function.Consumer;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.sstable.SSTableReader;
import org.crustee.raft.storage.sstable.SSTableWriter;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CrusteeTableTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_find_in_memtable() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            memtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            table.registerMemtable(memtable);

            SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
            assertThat(entry)
                    .isNotNull()
                    .hasSize(1);
        }
    }

    @Test
    public void should_merge_from_memtables() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable(1L);
            LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable(2L);
            ByteBuffer key = ByteBuffer.allocate(4);
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(12), ByteBuffer.allocate(16)));
            newerMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(32)));
            table.registerMemtable(olderMemtable);
            table.registerMemtable(newerMemtable);

            SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
            assertThat(entry)
                    .isNotNull()
                    .hasSize(2)
                    .containsExactly(
                            entry(ByteBuffer.allocate(8), ByteBuffer.allocate(32)),
                            entry(ByteBuffer.allocate(12), ByteBuffer.allocate(16))
                    );
        }
    }

    @Test
    public void should_find_in_sstable() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            memtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            table.registerMemtable(memtable);

            flushMemtableAndTest(table, memtable, reader -> {
                SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
                assertThat(entry)
                        .isNotNull()
                        .hasSize(1);
            });
        }
    }

    @Test
    public void should_merge_from_sstables() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(12), ByteBuffer.allocate(16)));
            table.registerMemtable(olderMemtable);

            LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable(2L);
            newerMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(32)));
            table.registerMemtable(newerMemtable);


            try (SSTableReader olderReader = flushMemtable(olderMemtable);
                 SSTableReader newerReader = flushMemtable(newerMemtable)) {
                table.memtableFlushed(olderMemtable, olderReader);
                table.memtableFlushed(newerMemtable, newerReader);

                SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
                assertThat(entry).isNotNull()
                        .containsOnly(
                                entry(ByteBuffer.allocate(8), ByteBuffer.allocate(32)),
                                entry(ByteBuffer.allocate(12), ByteBuffer.allocate(16))
                        );
            }
        }

    }

    @Test
    public void should_merge_from_sstables_and_memtable() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(12), ByteBuffer.allocate(16)));
            table.registerMemtable(olderMemtable);
            flushMemtableAndTest(table, olderMemtable, reader -> {
                LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable(1L);
                newerMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(32)));
                table.registerMemtable(newerMemtable);

                SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
                assertThat(entry).isNotNull()
                        .containsOnly(
                                entry(ByteBuffer.allocate(8), ByteBuffer.allocate(32)),
                                entry(ByteBuffer.allocate(12), ByteBuffer.allocate(16))
                        );
            });
        }
    }

    @Test
    public void should_return_null_when_not_found() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            SortedMap<ByteBuffer, ByteBuffer> map = table.get(ByteBuffer.allocate(4));
            assertThat(map).isNull();
        }
    }

    @Test
    public void should_replace_memtable_with_sstable() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            memtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            table.registerMemtable(memtable);

            LockFreeBTreeMemtable otherMemtable = new LockFreeBTreeMemtable(2L);
            table.registerMemtable(otherMemtable);

            assertThat(table.tables.memtables).hasSize(2).containsExactly(memtable, otherMemtable);

            flushMemtableAndTest(table, memtable, reader -> {
                assertThat(table.tables.memtables).containsOnly(otherMemtable);
                assertThat(table.tables.ssTableReaders).hasSize(1);
            });
        }
    }

    @Test
    public void should_reorder_tables_by_timestamp_when_flushed_in_disorder() throws Exception {
        try(CrusteeTable table = new CrusteeTable()) {
            LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable(1L);
            ByteBuffer key = ByteBuffer.allocate(4);
            olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
            table.registerMemtable(olderMemtable);

            LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable(2L);
            table.registerMemtable(newerMemtable);

            assertThat(table.tables.memtables).hasSize(2).containsExactly(olderMemtable, newerMemtable);

            // flush the newer before the older -> timestamp inversion
            try (SSTableReader newerTableReader = flushMemtable(newerMemtable);
                 SSTableReader olderTableReader = flushMemtable(olderMemtable)) {
                table.memtableFlushed(newerMemtable, newerTableReader);
                table.memtableFlushed(olderMemtable, olderTableReader);

                assertThat(table.tables.ssTableReaders).hasSize(2);
                assertThat(table.tables.ssTableReaders.get(0).getCreationTimestamp()).isEqualTo(olderMemtable.getCreationTimestamp());
                assertThat(table.tables.ssTableReaders.get(1).getCreationTimestamp()).isEqualTo(newerMemtable.getCreationTimestamp());
            }
        }
    }

    private SSTableReader flushMemtable(LockFreeBTreeMemtable memtable) throws IOException {
        Path tablePath = temporaryFolder.newFile().toPath();
        Path indexPath = temporaryFolder.newFile().toPath();
        try (SSTableWriter writer = new SSTableWriter(tablePath, indexPath, memtable)) {
            writer.write();
            return writer.toReader();
        }
    }

    private void flushMemtableAndTest(CrusteeTable table, LockFreeBTreeMemtable memtable, Consumer<SSTableReader> test) throws IOException {
        try (SSTableReader reader = flushMemtable(memtable)) {
            table.memtableFlushed(memtable, reader);
            test.accept(reader);
        }
    }
}