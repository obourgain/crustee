package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.crustee.raft.storage.row.Row;

public class Serializer {

    public static SerializedRow serialize(Row row) {
        Map<ByteBuffer, ByteBuffer> map = row.asMap();
        ByteBuffer[] buffers = new ByteBuffer[map.size() * 3];

        int i = 0;
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

}
