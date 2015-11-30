package org.crustee.raft.storage.sstable.index;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class IndexWithSummaryTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    InternalIndexReader indexReader;
    @Mock
    TreeMapIndexSummary indexSummary;

    @Test
    public void should_search_position_in_summary() {
        ByteBuffer key = ByteBuffer.allocate(4);
        IndexWithSummary indexWithSummary = new IndexWithSummary(indexReader, indexSummary);
        when(indexSummary.previousIndexEntryLocation(key)).thenReturn(42);
        when(indexSummary.getSamplingInterval()).thenReturn(54);

        indexWithSummary.findRowLocation(key);
        verify(indexSummary).previousIndexEntryLocation(key);
        verify(indexReader).findRowLocation(key, 42, 54);
    }
}