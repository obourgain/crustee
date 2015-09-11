package org.crustee.raft.storage.bloomfilter.bitset;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.utils.UncheckedIOUtils.openChannel;
import static org.crustee.raft.utils.UncheckedIOUtils.setPosition;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ByteBufferAccessorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void should_set_byte() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        new ByteBufferAccessor(buffer).set(1, (byte) 1);
        Assertions.assertThat(buffer.get(1)).isEqualTo((byte) 1);
    }

    @Test
    public void should_get_byte() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(1, (byte) 1);
        Assertions.assertThat(new ByteBufferAccessor(buffer).get(1)).isEqualTo((byte) 1);
    }

    @Test
    public void should_fail_when_out_of_bounds() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Throwable throwable = ThrowableAssert.catchThrowable(() -> new ByteBufferAccessor(buffer).set(10, (byte) 1));
        Assertions.assertThat(throwable).isInstanceOf(IndexOutOfBoundsException.class);

        Throwable throwable2 = ThrowableAssert.catchThrowable(() -> new ByteBufferAccessor(buffer).get(10));
        Assertions.assertThat(throwable2).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    public void should_serialize_deserialize() throws Exception {
        Path file = temporaryFolder.newFile().toPath();
        try (FileChannel channel = openChannel(file, WRITE, READ)) {
            int size = 4;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
            accessor.set(1, (byte) 1);
            accessor.writeTo(channel);
            setPosition(channel, 0);

            ByteBuffer deserBuffer = ByteBuffer.allocate(size);
            new ByteBufferAccessor(deserBuffer).readFrom(channel, size);

            assertThat(deserBuffer).isEqualTo(buffer);
        }
    }
}