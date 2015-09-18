package org.crustee.raft.storage.bloomfilter.bitset;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.co.real_logic.agrona.UnsafeAccess;

public class DirectByteBufferFactoryTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_wrap_memory() throws Exception {
        Assume.assumeTrue(DirectByteBufferFactory.AVAILABLE);
        int length = 10;
        long address = UnsafeAccess.UNSAFE.allocateMemory(length);
        UnsafeNativeAccessor accessor = new UnsafeNativeAccessor(address, length);
        for (int i = 0; i < 10; i++) {
            accessor.set(i, (byte) i);
        }

        ByteBuffer buffer = DirectByteBufferFactory.wrap(address, length);
        assertThat(buffer.position()).isEqualTo(0);
        assertThat(buffer.limit()).isEqualTo(length);
        assertThat(buffer.capacity()).isEqualTo(length);

        for (int i = 0; i < length; i++) {
            assertThat(buffer.get(i)).isEqualTo((byte) i);
        }
    }

    @Test
    public void should_be_writable_to_channel() throws Exception {
        Assume.assumeTrue(DirectByteBufferFactory.AVAILABLE);
        int length = 10;
        long address = UnsafeAccess.UNSAFE.allocateMemory(length);

        ByteBuffer buffer = DirectByteBufferFactory.wrap(address, length);
        FileChannel channel = UncheckedIOUtils.openChannel(temporaryFolder.newFile().toPath(), WRITE);
        int write = UncheckedIOUtils.write(channel, buffer);
        assertThat(write).isEqualTo(length);

    }
}