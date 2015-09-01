package org.crustee.raft.storage.sstable.index;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.sstable.AbstractSSTableTest;
import org.crustee.raft.storage.sstable.RowLocation;
import org.crustee.raft.storage.sstable.SSTableHeader;
import org.crustee.raft.storage.sstable.Serializer;
import org.junit.Test;

public class MmapIndexReaderTest extends AbstractSSTableTest {

    @Test
    public void should_find_correct_offsets() throws Exception {
        test(memtable(), this::initSstable,
                (writer, table, index) -> {

                    try (IndexReader reader = IndexReaderFactory.create(index.toPath())) {
                        ByteBuffer searchedKey = ByteBuffer.allocate(4).putShort(0, (short) 0);
                        RowLocation rowLocation = reader.findRowLocation(searchedKey);

                        assertThat(rowLocation.isFound());
                        assertThat((int) rowLocation.getRowKeySize()).isEqualTo(searchedKey.limit());
                        assertThat((int) rowLocation.getValueSize()).isEqualTo(Serializer.serializedSizeOverhead(1) + 2 * 4);
                        assertThat(rowLocation.getOffset()).isEqualTo(SSTableHeader.BUFFER_SIZE);
                        assertThat(rowLocation.getRowKeyOffset()).isEqualTo(SSTableHeader.BUFFER_SIZE
                                + 2 // key size
                                + 4 // value size
                        );
                        assertThat(rowLocation.getValueOffset()).isEqualTo(SSTableHeader.BUFFER_SIZE
                                + 2 // key size
                                + 4 // value size
                                + searchedKey.limit() // key
                        );
                    }
                },
                (writer, table, index) -> {

                    try (IndexReader reader = IndexReaderFactory.create(index.toPath())) {
                        ByteBuffer searchedKey = ByteBuffer.allocate(4).putShort(0, (short) 10);
                        RowLocation rowLocation = reader.findRowLocation(searchedKey);

                        assertThat(rowLocation.isFound());
                        assertThat((int) rowLocation.getRowKeySize()).isEqualTo(searchedKey.limit());
                        assertThat((int) rowLocation.getValueSize()).isEqualTo(Serializer.serializedSizeOverhead(1) + 2 * 4);
                        int expectedOffset = SSTableHeader.BUFFER_SIZE + 10 * (
                                +2 // key size
                                        + 4 // value size
                                        + searchedKey.limit() // key
                                        + Serializer.serializedSizeOverhead(1) + 2 * 4 // value
                        );

                        assertThat(rowLocation.getOffset()).isEqualTo(expectedOffset);
                        assertThat(rowLocation.getRowKeyOffset()).isEqualTo(expectedOffset
                                + 2 // key size
                                + 4 // value size
                        );
                        assertThat(rowLocation.getValueOffset()).isEqualTo(expectedOffset
                                + 2 // key size
                                + 4 // value size
                                + searchedKey.limit() // key
                        );
                    }
                });
    }

    @Test
    public void should_return_not_found() throws Exception {
        test(memtable(), this::initSstable,
                (writer, table, index) -> {
                    IndexReader reader = IndexReaderFactory.create(index.toPath());
                    ByteBuffer searchedKey = ByteBuffer.allocate(4).putShort(0, Short.MAX_VALUE);
                    RowLocation rowLocation = reader.findRowLocation(searchedKey);
                    Assertions.assertThat(rowLocation.isFound()).isFalse();

                }
        );
    }

    @Test
    public void should_reject_key_too_long() throws Exception {
        test(memtable(), this::initSstable,
                (writer, table, index) -> {
                    try (IndexReader reader = IndexReaderFactory.create(index.toPath())) {
                        ByteBuffer key = ByteBuffer.allocate(Short.MAX_VALUE + 1).putShort(0, (short) 0);

                        Throwable throwable = ThrowableAssert.catchThrowable(() -> reader.findRowLocation(key));
                        Assertions.assertThat(throwable).isInstanceOf(AssertionError.class)
                                .hasMessageContaining("key may not be longer than");
                    }

                }
        );
    }

    private WritableMemtable memtable() {
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable();
        IntStream.range(0, 100)
                .forEach(i ->
                        memtable.insert(ByteBuffer.allocate(4).putShort(0, (short) i),
                                singletonMap(
                                        ByteBuffer.allocate(4).putInt(0, i),
                                        ByteBuffer.allocate(4).putInt(0, i))));
        return memtable;
    }
}