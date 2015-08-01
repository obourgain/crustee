package org.crustee.raft.storage.row;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class MapRow implements Row {

    private volatile TreeMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
    private volatile long estimatedSizeInBytes = 0;

    public MapRow(Map<ByteBuffer, ByteBuffer> values) {
        map.putAll(values);
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet()) {
            estimatedSizeInBytes += entry.getKey().limit() + entry.getValue().limit();
        }
    }

    @Override
    public void addAll(Map<ByteBuffer, ByteBuffer> entries) {
        // this doesn't account for the size of the tree itself
        long addedSizeInBytes = 0;
        TreeMap<ByteBuffer, ByteBuffer> clone = (TreeMap<ByteBuffer, ByteBuffer>) map.clone();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet()) {
            ByteBuffer oldValue = clone.put(entry.getKey(), entry.getValue());
            if(oldValue != null) {
                // may be negative and so we just decrease the size
                addedSizeInBytes += entry.getValue().limit() - oldValue.limit();
            } else {
                addedSizeInBytes += entry.getKey().limit() + entry.getValue().limit();
            }
        }
        map = clone;
        estimatedSizeInBytes += addedSizeInBytes;
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> asMap() {
        return map;
    }

    @Override
    public long getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }
}
