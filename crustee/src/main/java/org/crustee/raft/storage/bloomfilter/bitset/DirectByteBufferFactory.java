package org.crustee.raft.storage.bloomfilter.bitset;

import static org.slf4j.LoggerFactory.getLogger;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import uk.co.real_logic.agrona.UnsafeAccess;

public class DirectByteBufferFactory {

    static final boolean AVAILABLE;
    private static final Logger logger = getLogger(UnsafeNativeAccessor.class);

    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
    private static final long addressOffset;
    private static final long capacityOffset;
    private static final long limitOffset;
    private static final long attOffset;

    private static final Unsafe unsafe = UnsafeAccess.UNSAFE;

    // set this in the attachment field to be able to check that a buffer was allocated from here and
    // avoid messing with the internal state of buffer allocated elsewhere
    private static final Object MARKER = new Object();

    static {
        boolean tempAvailable = true;
        long tempAddressOffset = 0;
        long tempCapacityOffset = 0;
        long tempLimitOffset = 0;
        long tempAttOffset = 0;
        Class<?> tempDirectByteBufferClass = null;
        try {
            // some fields are declared on parent classes
            Class<?> bufferClass = Class.forName("java.nio.Buffer");
            Field address = bufferClass.getDeclaredField("address");
            tempAddressOffset = UnsafeAccess.UNSAFE.objectFieldOffset(address);
            Field capacity = bufferClass.getDeclaredField("capacity");
            tempCapacityOffset = UnsafeAccess.UNSAFE.objectFieldOffset(capacity);
            Field limit = bufferClass.getDeclaredField("limit");
            tempLimitOffset = UnsafeAccess.UNSAFE.objectFieldOffset(limit);

            Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
            Field att = directBufferClass.getDeclaredField("att");
            tempAttOffset = UnsafeAccess.UNSAFE.objectFieldOffset(att);
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
        attOffset = tempAttOffset;
        AVAILABLE = tempAvailable;
        DIRECT_BYTE_BUFFER_CLASS = tempDirectByteBufferClass;
    }

    // to fill properly before use or it will crash horribly the vm
    public static ByteBuffer allocateEmpty() {
        try {
            ByteBuffer buffer = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(buffer, addressOffset, 0);
            unsafe.putInt(buffer, capacityOffset, 0);
            unsafe.putInt(buffer, limitOffset, 0);
            unsafe.putObject(buffer, attOffset, MARKER);
            buffer.order(ByteOrder.nativeOrder());
            return buffer;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer allocate(int size) {
        try {
            long address = UnsafeAccess.UNSAFE.allocateMemory(size);
            ByteBuffer buffer = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(buffer, addressOffset, address);
            unsafe.putInt(buffer, capacityOffset, size);
            unsafe.putInt(buffer, limitOffset, size);
            unsafe.putObject(buffer, attOffset, MARKER);
            buffer.order(ByteOrder.nativeOrder());
            return buffer;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer wrap(long address, int length) {
        try {
            ByteBuffer buffer = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            unsafe.putLong(buffer, addressOffset, address);
            unsafe.putInt(buffer, capacityOffset, length);
            unsafe.putInt(buffer, limitOffset, length);
            unsafe.putObject(buffer, attOffset, MARKER);
            buffer.order(ByteOrder.nativeOrder());
            return buffer;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer wrap(ByteBuffer buffer, long address, int length) {
        checkAllocatedHere(buffer);
        unsafe.putLong(buffer, addressOffset, address);
        unsafe.putInt(buffer, capacityOffset, length);
        unsafe.putInt(buffer, limitOffset, length);
        buffer.order(ByteOrder.nativeOrder());
        return buffer;
    }

    public static ByteBuffer slice(ByteBuffer bufferToSlice, ByteBuffer slice, int start, int length) {
        checkAllocatedHere(bufferToSlice);
        checkAllocatedHere(slice);
        assert length <= bufferToSlice.capacity();
        long startAddress = unsafe.getLong(bufferToSlice, addressOffset);

        long sliceStartAddress = startAddress + start;

        unsafe.putLong(slice, addressOffset, sliceStartAddress);
        unsafe.putInt(slice, capacityOffset, length);
        unsafe.putInt(slice, limitOffset, length);
        slice.order(bufferToSlice.order());
        slice.position(0);
        return slice;
    }

    public static void free(ByteBuffer buffer) {
        checkAllocatedHere(buffer);
        UnsafeAccess.UNSAFE.freeMemory(((DirectBuffer) buffer).address());
    }

    public static ByteBuffer reallocate(ByteBuffer buffer, int newSize) {
        checkAllocatedHere(buffer);
        long currentAddress = ((DirectBuffer) buffer).address();
        long newAddress = UnsafeAccess.UNSAFE.reallocateMemory(currentAddress, newSize);
        return wrap(buffer, newAddress, newSize);
    }

    private static void checkAllocatedHere(ByteBuffer buffer) {
        assert buffer.isDirect();
        assert ((DirectBuffer) buffer).attachment() == MARKER;
    }
}
