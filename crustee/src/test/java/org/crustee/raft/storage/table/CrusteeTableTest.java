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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CrusteeTableTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_find_in_memtable() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        ByteBuffer key = ByteBuffer.allocate(4);
        memtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
        table.registerMemtable(memtable);

        SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
        assertThat(entry)
                .isNotNull()
                .hasSize(1);
    }

    @Test
    public void should_merge_from_memtables() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable();
        LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable();
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

    @Test
    public void should_find_in_sstable() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
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

    @Test
    public void should_merge_from_sstables() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable();
        ByteBuffer key = ByteBuffer.allocate(4);
        olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
        olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(12), ByteBuffer.allocate(16)));
        table.registerMemtable(olderMemtable);
        SSTableReader olderReader = flushMemtable(olderMemtable);
        table.memtableFlushed(olderMemtable, olderReader);

        LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable();
        newerMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(32)));
        table.registerMemtable(newerMemtable);
        SSTableReader newerReader = flushMemtable(newerMemtable);
        table.memtableFlushed(newerMemtable, newerReader);

        SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
        assertThat(entry).isNotNull()
                .containsOnly(
                        entry(ByteBuffer.allocate(8), ByteBuffer.allocate(32)),
                        entry(ByteBuffer.allocate(12), ByteBuffer.allocate(16))
                );
    }

    @Test
    public void should_merge_from_sstables_and_memtable() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable olderMemtable = new LockFreeBTreeMemtable();
        ByteBuffer key = ByteBuffer.allocate(4);
        olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
        olderMemtable.insert(key, singletonMap(ByteBuffer.allocate(12), ByteBuffer.allocate(16)));
        table.registerMemtable(olderMemtable);
        SSTableReader olderReader = flushMemtable(olderMemtable);
        table.memtableFlushed(olderMemtable, olderReader);

        LockFreeBTreeMemtable newerMemtable = new LockFreeBTreeMemtable();
        newerMemtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(32)));
        table.registerMemtable(newerMemtable);

        SortedMap<ByteBuffer, ByteBuffer> entry = table.get(key);
        assertThat(entry).isNotNull()
                .containsOnly(
                        entry(ByteBuffer.allocate(8), ByteBuffer.allocate(32)),
                        entry(ByteBuffer.allocate(12), ByteBuffer.allocate(16))
                );
    }

    @Test
    public void should_return_null_when_not_found() throws Exception {
        CrusteeTable table = new CrusteeTable();
        SortedMap<ByteBuffer, ByteBuffer> map = table.get(ByteBuffer.allocate(4));
        assertThat(map).isNull();
    }

    @Test
    public void should_replace_memtable_with_sstable() throws Exception {
        CrusteeTable table = new CrusteeTable();
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        ByteBuffer key = ByteBuffer.allocate(4);
        memtable.insert(key, singletonMap(ByteBuffer.allocate(8), ByteBuffer.allocate(16)));
        table.registerMemtable(memtable);

        LockFreeBTreeMemtable otherMemtable = new LockFreeBTreeMemtable();
        table.registerMemtable(otherMemtable);

        assertThat(table.memtables).hasSize(2).containsExactly(memtable, otherMemtable);

        flushMemtableAndTest(table, memtable, reader -> {
            assertThat(table.memtables).containsOnly(otherMemtable);
            assertThat(table.ssTableReaders).hasSize(1);
        });
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