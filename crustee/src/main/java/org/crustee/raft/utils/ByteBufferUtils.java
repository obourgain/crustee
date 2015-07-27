package org.crustee.raft.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;
import uk.co.real_logic.agrona.BitUtil;

public class ByteBufferUtils {

    public static String toHexString(ByteBuffer buffer) {
        return toHexString(buffer, 0, buffer.limit());
    }

    public static String toHexStringFullDump(ByteBuffer buffer) {
        return toHexString(buffer, 0, buffer.capacity());
    }

    public static String toHexString(ByteBuffer buffer, int src, int length) {
        int currentPos = buffer.position();
        byte[] bytes = new byte[length];
        buffer.position(src);
        buffer.get(bytes, 0, length);
        buffer.position(currentPos);
        return BitUtil.toHex(bytes);
    }

    public static ByteBuffer bufferOfSize(ByteBuffer buffer, int size) {
        if(buffer.capacity() >= size) {
            buffer.position(0);
            buffer.limit(size);
            return buffer;
        }
        return ByteBuffer.allocate(size);
    }

    private static final Comparator<ByteBuffer> lengthFirstComparator = new Comparator<ByteBuffer>() {
        @Override
        public int compare(ByteBuffer o1, ByteBuffer o2) {
            assert o1.position() == 0 : "only works when buffer's position is 0";
            assert o2.position() == 0 : "only works when buffer's position is 0";
            int o1Remaining = o1.remaining();
            int o2Remaining = o2.remaining();
            if(o1Remaining < o2Remaining) {
                return -1;
            }
            if(o1Remaining > o2Remaining) {
                return 1;
            }
            // buffers have the same length, so this is safe
            for (int i = 0; i < o1.limit(); i++) {
                int cmp = Byte.compare(o1.get(i), o2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    public static Comparator<ByteBuffer> lengthFirstComparator() {
        return lengthFirstComparator;
    }
}
