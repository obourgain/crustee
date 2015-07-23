package org.crustee.raft.storage.row;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

public class MapRow implements Row {

    private volatile TreeMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    public MapRow(Map<ByteBuffer, ByteBuffer> values) {
        map.putAll(values);
    }

    public void addAll(Map<ByteBuffer, ByteBuffer> entries) {
        TreeMap<ByteBuffer, ByteBuffer> clone = (TreeMap<ByteBuffer, ByteBuffer>) map.clone();
        clone.putAll(entries);
        map = clone;
    }

    @Override
    public Map<ByteBuffer, ByteBuffer> asMap() {
        return map;
    }

}
