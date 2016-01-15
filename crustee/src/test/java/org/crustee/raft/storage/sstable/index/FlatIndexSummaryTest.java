package org.crustee.raft.storage.sstable.index;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.sstable.AbstractSSTableTest;
import org.crustee.raft.storage.sstable.SSTableWriter;
import org.junit.Test;

public class FlatIndexSummaryTest extends AbstractSSTableTest {

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
        FlatIndexSummary indexSummary = FlatIndexSummary.builder(indexReader, 128).build();

        int currentKeyAtGivenSamplingInterval = 0;
        for (int i = 0; i < MEMTABLE_SIZE; i++) {
            if(i % indexSummary.getSamplingInterval() == 0 && i != 0) {
                currentKeyAtGivenSamplingInterval += indexSummary.getSamplingInterval();
            }
            int position = indexSummary.previousIndexEntryLocation(ByteBuffer.allocate(keySize).putShort(0, (short) i));
            int expectedPosition = currentKeyAtGivenSamplingInterval * (IndexWriter.INDEX_ENTRY_KEY_OFFSET_SIZE_LENGTH + keySize);
            assertThat(position)
                    .describedAs("index %s currentKeyAtGivenSamplingInterval %s", i, currentKeyAtGivenSamplingInterval)
                    .isEqualTo(expectedPosition);
        }
    }

    private void should_apply_correct_sampling_interval(SSTableWriter writer, File table, File index) {
        int keySize = 4;
        int expectedSamplingInterval = 128;
        MmapIndexReader indexReader = new MmapIndexReader(index.toPath());
        FlatIndexSummary indexSummary = FlatIndexSummary.builder(indexReader, 128).build();

        assertThat(indexSummary.getSamplingInterval()).isEqualTo(expectedSamplingInterval);
        assertThat(indexSummary.positions.size()).isEqualTo(MEMTABLE_SIZE / expectedSamplingInterval + 1);

        for (int currentKeyAtGivenSamplingInterval = 0, summaryIndex = 0; currentKeyAtGivenSamplingInterval < MEMTABLE_SIZE; currentKeyAtGivenSamplingInterval += indexSummary.getSamplingInterval(), summaryIndex++) {
            // can't use contains because IntArrayList implements Iterable<IntCursor> but contains ints
            assertThat(indexSummary.positions.get(summaryIndex))
                    .describedAs("index %d", summaryIndex)
                    .isEqualTo(summaryIndex * (keySize + Long.BYTES)); // long is pointer to entry in index file
            assertThat(indexSummary.keyAt(summaryIndex)).describedAs("index %d", summaryIndex).isEqualTo(ByteBuffer.allocate(keySize).putShort(0, (short) currentKeyAtGivenSamplingInterval));
            assertThat(indexSummary.pointerToIndexEntry(summaryIndex)).describedAs("index %d", summaryIndex);
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

    @Test
    public void name() throws Exception {
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if(i % 128 != (i & (128 - 1))) {
                fail("" + i);
            }
        }


    }
}
