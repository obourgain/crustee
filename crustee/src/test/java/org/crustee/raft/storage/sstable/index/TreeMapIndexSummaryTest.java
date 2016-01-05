package org.crustee.raft.storage.sstable.index;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.sstable.AbstractSSTableTest;
import org.crustee.raft.storage.sstable.SSTableWriter;
import org.junit.Test;

public class TreeMapIndexSummaryTest extends AbstractSSTableTest {

    private static final int MEMTABLE_SIZE = 2000;

    @Test
    public void bunchOfTests() throws Exception {
        test(memtable(), this::initSstable,
                this::should_apply_correct_sampling_interval,
                this::should_get_correct_lower_position);
    }

    private void should_get_correct_lower_position(SSTableWriter writer, File table, File index) {
        int keySize = 4;
        MmapIndexReader indexReader = new MmapIndexReader(index.toPath());
        TreeMapIndexSummary indexSummary = new TreeMapIndexSummary(indexReader, 128);

        int position = indexSummary.previousIndexEntryLocation(ByteBuffer.allocate(keySize).putShort(0, (short) 130));
        int expectedPosition = 128 * (IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH + keySize);
        assertThat(position).isEqualTo(expectedPosition);
    }

    private void should_apply_correct_sampling_interval(SSTableWriter writer, File table, File index) {
        int keySize = 4;
        int expectedSamplingInterval = 128;
        MmapIndexReader indexReader = new MmapIndexReader(index.toPath());
        TreeMapIndexSummary indexSummary = new TreeMapIndexSummary(indexReader, expectedSamplingInterval);

        assertThat(indexSummary.getSamplingInterval()).isEqualTo(expectedSamplingInterval);
        assertThat(indexSummary.positions.size()).isEqualTo(MEMTABLE_SIZE / expectedSamplingInterval + 1);
//        indexSummary.positions.forEach((k, v) -> System.out.println(k.getShort(0) + " -> " + v));
        for (int i = 0; i < MEMTABLE_SIZE; i += indexSummary.getSamplingInterval()) {
            assertThat(indexSummary.positions)
                    .describedAs("index %d", i)
                    .contains(entry(
                            ByteBuffer.allocate(keySize).putShort(0, (short) i),
                            i * (IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH + keySize))
                    );
        }
    }

    private WritableMemtable memtable() {
        LockFreeBTreeMemtable memtable = new LockFreeBTreeMemtable(1L);
        IntStream.range(0, MEMTABLE_SIZE)
                .forEach(i ->
                        memtable.insert(ByteBuffer.allocate(4).putShort(0, (short) i),
                                singletonMap(
                                        ByteBuffer.allocate(4).putInt(0, i),
                                        ByteBuffer.allocate(4).putInt(0, i))));
        return memtable;
    }

}
