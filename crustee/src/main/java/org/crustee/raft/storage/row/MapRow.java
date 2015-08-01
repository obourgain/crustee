package org.crustee.raft.storage.row;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class MapRow implements Row {

    private volatile long estimatedSizeInBytes = 0;
    private volatile TreeMap<ByteBuffer, ByteBuffer> map;

    public MapRow(Map<ByteBuffer, ByteBuffer> values) {
        map = new TreeMap<>();
        map.putAll(values);
        estimatedSizeInBytes = size(map);
    }

    public MapRow(TreeMap<ByteBuffer, ByteBuffer> values, boolean copy) {
        if (copy) {
            this.map = new TreeMap<>();
            this.map.putAll(values);
        } else {
            this.map = values;
        }
        estimatedSizeInBytes = size(map);
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

    private long size(Map<ByteBuffer, ByteBuffer> map) {
        long size = 0;
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : map.entrySet()) {
            size += entry.getKey().limit() + entry.getValue().limit();
        }
        return size;
    }
}
