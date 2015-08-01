package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.crustee.raft.storage.row.MapRow;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.ByteBufferUtils;

public class Serializer {

    public static SerializedRow serialize(Row row) {
        Map<ByteBuffer, ByteBuffer> map = row.asMap();
        ByteBuffer[] buffers = new ByteBuffer[map.size() * 3 + 1];
        buffers[0] = ByteBuffer.allocate(4).putInt(0, map.size());

        int i = 1;
        Set<Map.Entry<ByteBuffer, ByteBuffer>> entrySet = map.entrySet();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : entrySet) {
            assert entry.getKey().position() == 0;
            assert entry.getValue().position() == 0;
            buffers[i++] = (ByteBuffer) ByteBuffer.allocate(Short.BYTES + Integer.BYTES)
                    .putShort((short) entry.getKey().limit()) // key size
                    .putInt(entry.getValue().limit())
                    .flip(); // value size
            // duplicate to avoid position/limit madness
            buffers[i++] = entry.getKey().duplicate();
            buffers[i++] = entry.getValue().duplicate();
        }
        return new SerializedRow(buffers);
    }

    public static Row deserialize(SerializedRow serializedRow) {
        ByteBuffer[] buffers = serializedRow.getBuffers();
        // only deserialize on disk value for now, so a single buffer
        assert buffers.length == 1;

        ByteBuffer buffer = buffers[0];
        int entries = buffer.getInt();
        TreeMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();
        for (int i = 0; i < entries; i++) {
            short keySize = buffer.getShort();
            int valueSize = buffer.getInt();
            ByteBuffer key = (ByteBuffer) buffer.slice().limit(keySize);
            ByteBufferUtils.advance(buffer, keySize);
            ByteBuffer value = (ByteBuffer) buffer.slice().limit(valueSize);
            ByteBufferUtils.advance(buffer, valueSize);
            map.put(key, value);
        }
        return new MapRow(map, false);
    }

}
