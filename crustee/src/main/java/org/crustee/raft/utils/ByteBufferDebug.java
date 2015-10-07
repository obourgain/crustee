package org.crustee.raft.utils;

import java.nio.ByteBuffer;
import uk.co.real_logic.agrona.BitUtil;

public class ByteBufferDebug {

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

}
