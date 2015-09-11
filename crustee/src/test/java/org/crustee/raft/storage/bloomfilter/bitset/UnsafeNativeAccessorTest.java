package org.crustee.raft.storage.bloomfilter.bitset;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.setPosition;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.real_logic.agrona.UnsafeAccess;

public class UnsafeNativeAccessorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_set_byte() throws Exception {
        test((address, length) -> {
            new UnsafeNativeAccessor(address, length).set(1, (byte) 1);
            assertThat(UnsafeAccess.UNSAFE.getByte(address + 1)).isEqualTo((byte) 1);
        });
    }

    @Test
    public void should_get_byte() throws Exception {
        test((address, length) -> {
            new UnsafeNativeAccessor(address, length).set(1, (byte) 1);
            assertThat(UnsafeAccess.UNSAFE.getByte(address + 1)).isEqualTo((byte) 1);
        });
    }

    @Test
    public void should_fail_when_out_of_bounds() throws Exception {
        Assume.assumeFalse(Boolean.getBoolean("bitset.unsafe.boundcheck.disabled")); // avoid crashing the VM if bound check is not enabled
        test((address, length) -> {
            Throwable throwable = ThrowableAssert.catchThrowable(() -> new UnsafeNativeAccessor(address, length).get(10));
            assertThat(throwable).isInstanceOf(IndexOutOfBoundsException.class);
        });
        test((address, length) -> {
            Throwable throwable = ThrowableAssert.catchThrowable(() -> new UnsafeNativeAccessor(address, length).set(10, (byte) 1));
            assertThat(throwable).isInstanceOf(IndexOutOfBoundsException.class);
        });
    }

    @Test
    public void should_serialize_deserialize() throws Exception {
        Path file = temporaryFolder.newFile().toPath();
        try(FileChannel channel = openChannel(file, WRITE, READ)) {
            test((address, length) -> {
                UnsafeNativeAccessor accessor = new UnsafeNativeAccessor(address, length);
                accessor.set(1, (byte) 1);
                accessor.writeTo(channel);
                setPosition(channel, 0);

                long deserAddress = UnsafeAccess.UNSAFE.allocateMemory(length);
                new UnsafeNativeAccessor(deserAddress, length).readFrom(channel, length);

                ByteBuffer serBuffer = DirectByteBufferFactory.wrap(address, length);
                ByteBuffer deserBuffer = DirectByteBufferFactory.wrap(deserAddress, length);
                assertThat(deserBuffer).isEqualTo(serBuffer);
            });
        }
    }

    private void test(BiConsumer<Long, Integer> action) {
        int length = 4;
        long address = allocate(length);
        try {
            action.accept(address, length);
        } finally {
            free(address);
        }
    }

    private long allocate(long length) {
        long address = UnsafeAccess.UNSAFE.allocateMemory(length);
        UnsafeAccess.UNSAFE.setMemory(address, length, (byte) 0);
        return address;
    }

    private void free(long address) {
        UnsafeAccess.UNSAFE.freeMemory(address);
    }

}