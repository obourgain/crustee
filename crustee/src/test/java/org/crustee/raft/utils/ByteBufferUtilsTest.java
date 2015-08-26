package org.crustee.raft.utils;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import org.junit.Test;

public class ByteBufferUtilsTest {

    @Test
    public void buffer_equality() throws Exception {
        ByteBuffer bb1 = ByteBuffer.allocate(42).put(10, (byte) 42);
        ByteBuffer bb2 = ByteBuffer.allocate(42).put(10, (byte) 42);

        assertThat(ByteBufferUtils.equals(bb1, bb2)).isTrue();
        assertThat(ByteBufferUtils.equals(bb1, bb2, bb2.position(), bb2.limit())).isTrue();
        assertThat(ByteBufferUtils.equals(bb1, bb1.position(), bb1.limit(), bb2, bb2.position(), bb2.limit())).isTrue();
    }

    @Test
    public void buffer_equality_starting_from_non_0_index() throws Exception {
        ByteBuffer bb1 = ByteBuffer.allocate(40).put(10, (byte) 42);
        ByteBuffer bb2 = ByteBuffer.allocate(100).put(30, (byte) 42);
        bb2.position(20).limit(60);

        assertThat(ByteBufferUtils.equals(bb1, bb2)).isTrue();
        assertThat(ByteBufferUtils.equals(bb1, bb2, bb2.position(), bb2.limit())).isTrue();
        assertThat(ByteBufferUtils.equals(bb1, bb1.position(), bb1.limit(), bb2, bb2.position(), bb2.limit())).isTrue();
    }
}