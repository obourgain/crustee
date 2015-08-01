package org.crustee.raft.storage.memtable;

import static java.nio.ByteBuffer.allocate;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.data.MapEntry;
import org.junit.Test;

public class LockFreeBTreeMemtableTest {

    @Test
    public void should_insert() throws Exception {
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        memtable.insert(allocate(42), singletonMap(allocate(43), allocate(1024)));

        assertThat(memtable.get(allocate(42)).asMap()).isEqualTo(singletonMap(allocate(43), allocate(1024)));
    }

    @Test
    public void should_merge() throws Exception {
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        memtable.insert(allocate(42), singletonMap(allocate(1), allocate(1024)));
        memtable.insert(allocate(42), singletonMap(allocate(2), allocate(2048)));

        assertThat(memtable.get(allocate(42)).asMap()).contains(
                MapEntry.entry(allocate(1), allocate(1024)),
                MapEntry.entry(allocate(2), allocate(2048))
        );

        assertThat(memtable.getEstimatedSizeInBytes()).isEqualTo(42 + 1 + 1024 + 2 + 2048);
    }

    @Test
    public void should_overwritten_in_value() throws Exception {
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        memtable.insert(allocate(42), singletonMap(allocate(1), allocate(1024)));
        memtable.insert(allocate(42), singletonMap(allocate(1), allocate(2048)));

        assertThat(memtable.get(allocate(42)).asMap()).contains(MapEntry.entry(allocate(1), allocate(2048)));

        assertThat(memtable.getEstimatedSizeInBytes()).isEqualTo(42 + 1 + 2048);
    }
}