package org.crustee.raft.storage.commitlog;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import org.junit.Test;

public class CommitLogTest {

    @Test
    public void should_return_total_size_of_buffers() throws Exception {
        ByteBuffer[] buffers = new ByteBuffer[10];
        buffers[0] = (ByteBuffer) ByteBuffer.allocate(8).putLong(12).flip();
        buffers[1] = (ByteBuffer) ByteBuffer.allocate(4).putInt(42).flip();

        int size = CommitLog.buffersSize(buffers, 2);

        assertThat(size).isEqualTo(12);
    }
}