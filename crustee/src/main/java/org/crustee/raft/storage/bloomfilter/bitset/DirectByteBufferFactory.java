package org.crustee.raft.storage.bloomfilter.bitset;

import static org.slf4j.LoggerFactory.getLogger;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import sun.misc.Unsafe;
import uk.co.real_logic.agrona.UnsafeAccess;

public class DirectByteBufferFactory {

    static final boolean AVAILABLE;
    private static final Logger logger = getLogger(UnsafeNativeAccessor.class);

    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long addressOffset;
    private static final long capacityOffset;
    private static final long limitOffset;
    public static final Unsafe unsafe = UnsafeAccess.UNSAFE;

    static {
        boolean tempAvailable = true;
        long tempAddressOffset = 0;
        long tempCapacityOffset = 0;
        long tempLimitOffset = 0;
        Class<?> tempDirectByteBufferClass = null;
        try {
            Class<?> directBufferClass = Class.forName("java.nio.Buffer");
            Field address = directBufferClass.getDeclaredField("address");
            tempAddressOffset = UnsafeAccess.UNSAFE.objectFieldOffset(address);
            Field capacity = directBufferClass.getDeclaredField("capacity");
            tempCapacityOffset = UnsafeAccess.UNSAFE.objectFieldOffset(capacity);
            Field limit = directBufferClass.getDeclaredField("limit");
            tempLimitOffset = UnsafeAccess.UNSAFE.objectFieldOffset(limit);
        } catch (ClassNotFoundException e) {
            logger.warn("Can not find java.nio.Buffer class, " + UnsafeNativeAccessor.class.getSimpleName() + " is disabled. {}", e.getMessage());
            tempAvailable = false;
        } catch (NoSuchFieldException e) {
            logger.warn("java.nio.Buffer class structure is unexpected, " + UnsafeNativeAccessor.class.getSimpleName() + " is disabled. {}", e.getMessage());
            tempAvailable = false;
        }

        try {
            tempDirectByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
        } catch (ClassNotFoundException e) {
            logger.warn("Can not find java.nio.DirectByteBuffer class, " + UnsafeNativeAccessor.class.getSimpleName() + " is disabled. {}", e.getMessage());
            tempAvailable = false;
        }
        addressOffset = tempAddressOffset;
        capacityOffset = tempCapacityOffset;
        limitOffset = tempLimitOffset;
        AVAILABLE = tempAvailable;
        DIRECT_BYTE_BUFFER_CLASS = tempDirectByteBufferClass;
    }


    public static ByteBuffer wrap(long address, int length) {
        try {
            ByteBuffer buffer = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(buffer, addressOffset, address);
            unsafe.putInt(buffer, capacityOffset, length);
            unsafe.putInt(buffer, limitOffset, length);
            buffer.order(ByteOrder.nativeOrder());
            return buffer;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

}
