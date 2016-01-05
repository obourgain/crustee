package org.crustee.raft.storage.sstable.index;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.utils.ByteBufferUtils;

class TreeMapIndexSummary {

    private final int samplingInterval;
    @VisibleForTesting
    final TreeMap<ByteBuffer, Integer> positions = new TreeMap<>(ByteBufferUtils.lengthFirstComparator());

    TreeMapIndexSummary(MmapIndexReader indexReader, int samplingInterval) {
        this.samplingInterval = samplingInterval;
        load(indexReader);
    }

    int previousIndexEntryLocation(ByteBuffer key) {
        // there is a copy of the entry in TreeMap.floorEntry, we may be able to avoid the allocation if there is a way to return only the value
        Map.Entry<ByteBuffer, Integer> position = positions.floorEntry(key);
        if (position == null) {
            // searched key is lower than what we have in the summary, so it is before
            return 0;
        }
        return position.getValue();
    }

    private void load(MmapIndexReader indexReader) {
        indexReader.iterate(this::filter, this::callback);
    }

    private boolean filter(ByteBuffer byteBuffer, int integer) {
        return integer % samplingInterval == 0;
    }

    private void callback(ByteBuffer key, int position) {
        positions.put(key, position);
    }

    int getSamplingInterval() {
        return samplingInterval;
    }
}
