package org.crustee.raft.utils;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Comparator;
import org.slf4j.Logger;
import com.google.common.primitives.Longs;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

public class ByteBufferUtils {

    private static final Logger logger = getLogger(ByteBufferUtils.class);

    public static ByteBuffer advance(ByteBuffer buffer, int steps) {
        int newPosition = buffer.position() + steps;
        assert newPosition <= buffer.limit();
        buffer.position(newPosition);
        return buffer;
    }

    private static final Comparator<ByteBuffer> lengthFirstComparator = new Comparator<ByteBuffer>() {
        @Override
        public int compare(ByteBuffer o1, ByteBuffer o2) {
            assert o1.position() == 0 : "only works when buffer's position is 0";
            assert o2.position() == 0 : "only works when buffer's position is 0";
            int o1Remaining = o1.remaining();
            int o2Remaining = o2.remaining();
            if (o1Remaining < o2Remaining) {
                return -1;
            }
            if (o1Remaining > o2Remaining) {
                return 1;
            }
            // buffers have the same length, so this is safe
            int longComparisons = o1.limit() & ~7;
            int i = 0;
            for (; i < longComparisons ; i += Longs.BYTES) {
                int cmp = Long.compare(o1.getLong(i), o2.getLong(i));
                if (cmp != 0) {
                    return cmp;
                }
            }

            for (; i < o1.limit(); i++) {
                int cmp = Byte.compare(o1.get(i), o2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    };

    public static boolean equals(ByteBuffer bb1, ByteBuffer bb2) {
        return equals(bb1, bb2, bb2.position(), bb2.limit());
    }

    public static boolean equals(ByteBuffer bb1, ByteBuffer bb2, int start2, int end2) {
        return equals(bb1, bb1.position(), bb1.limit(), bb2, start2, end2);
    }

    public static boolean equals(ByteBuffer bb1, int start1, int end1, ByteBuffer bb2, int start2, int end2) {
        int length1 = end1 - start1;
        int length2 = end2 - start2;
        if (length1 != length2) {
            return false;
        }
        for (int i = 0; i < length1; i++) {
            byte b1 = bb1.get(i + start1);
            byte b2 = bb2.get(i + start2);
            if (b1 != b2) {
                return false;
            }
        }
        return true;
    }

    private static boolean DIRECT_BUFFER_CLEANING_ACTIVATED;

    static {
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);
            Cleaner cleaner = ((DirectBuffer) byteBuffer).cleaner();
            cleaner.clean();
            DIRECT_BUFFER_CLEANING_ACTIVATED = true;
            logger.info("DirectByteBuffer cleaning is enabled");
        } catch (Throwable throwable) {
            logger.warn("DirectByteBuffer cleaning is disabled, this may cause GC issues : {}", throwable.getMessage());
        }
    }

    public static boolean tryUnmap(MappedByteBuffer byteBuffer) {
        if (!DIRECT_BUFFER_CLEANING_ACTIVATED) {
            return false;
        }
        try {
            Cleaner cleaner = ((DirectBuffer) byteBuffer).cleaner();
            if (cleaner != null) {
                cleaner.clean();
            } else {
                logger.info("No cleaner for {}", byteBuffer.getClass());
            }
            return true;
        } catch (Throwable throwable) {
            logger.warn("An error occurred during DirectByteBuffer cleaning", throwable.getMessage());
            return false;
        }
    }

    public static Comparator<ByteBuffer> lengthFirstComparator() {
        return lengthFirstComparator;
    }

    public static String toString(ByteBuffer buffer) {
        return toString(buffer, buffer.position(), buffer.limit());
    }

    public static String toString(ByteBuffer buffer, int start, int end) {
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < end; i++) {
            builder.append(buffer.get(i)).append(" ");
        }
        return builder.toString();
    }
}
