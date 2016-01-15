package org.crustee.raft.storage.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.storage.btree.ArrayUtils.lastElement;
import static org.crustee.raft.storage.sstable.BufferingChannel.BUFFERED_CHANNEL_BUFFER_SIZE;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class BufferingChannelTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    FileChannel delegate;

    @Test
    public void should_not_write_when_buffering() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int written = bufferingChannel.write(buffer);
        assertThat(written).isEqualTo(bufferSize);
        verify(delegate, never()).write(any(ByteBuffer.class));
        assertThat(bufferingChannel.bufferIndex).isEqualTo(1);
        assertThat(bufferingChannel.buffer[0]).isSameAs(buffer);
    }

    @Test
    public void should_flush_write_when_buffer_is_full() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        for (int i = 0; i < BUFFERED_CHANNEL_BUFFER_SIZE - 1; i++) {
            bufferingChannel.write(ByteBuffer.allocate(bufferSize));
        }
        verifyNoMoreInteractions(delegate);
        assertThat(bufferingChannel.bufferIndex).isEqualTo(BUFFERED_CHANNEL_BUFFER_SIZE - 1);

        // trigger flush
        bufferingChannel.write(ByteBuffer.allocate(bufferSize));
        assertThat(bufferingChannel.bufferIndex).isEqualTo(0);
        // should write with vector io, not single call
        verify(delegate).write(any(ByteBuffer[].class), eq(0), eq(BUFFERED_CHANNEL_BUFFER_SIZE));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void should_flush_write_when_requested() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        bufferingChannel.write(ByteBuffer.allocate(bufferSize));
        verifyNoMoreInteractions(delegate);

        bufferingChannel.flush();
        assertThat(bufferingChannel.bufferIndex).isEqualTo(0);
        // should write with vector io, not single call
        verify(delegate).write(any(ByteBuffer[].class), eq(0), eq(1));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void should_buffer_when_several_buffer_added() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        ByteBuffer buffer1 = ByteBuffer.allocate(bufferSize);
        ByteBuffer buffer2 = ByteBuffer.allocate(bufferSize);
        bufferingChannel.write(new ByteBuffer[]{buffer1, buffer2});
        verifyNoMoreInteractions(delegate);

        assertThat(bufferingChannel.bufferIndex).isEqualTo(2);
        assertThat(bufferingChannel.buffer[0]).isSameAs(buffer1);
        assertThat(bufferingChannel.buffer[1]).isSameAs(buffer2);
    }

    @Test
    public void should_correctly_handle_several_buffer_when_flushing_in_the_middle_of_adding_them() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        for (int i = 0; i < BUFFERED_CHANNEL_BUFFER_SIZE - 1; i++) {
            bufferingChannel.write(ByteBuffer.allocate(bufferSize));
        }

        ByteBuffer buffer1 = ByteBuffer.allocate(bufferSize);
        ByteBuffer buffer2 = ByteBuffer.allocate(bufferSize);
        // first buffer triggers the flush, so we must have only buffer2 after
        bufferingChannel.write(new ByteBuffer[]{buffer1, buffer2});

        // we have the correct ByteBuffer in the buffer
        assertThat(bufferingChannel.bufferIndex).isEqualTo(1);
        assertThat(bufferingChannel.buffer[0]).isSameAs(buffer2);

        // we have written the buffer triggering the flush
        ArgumentCaptor<ByteBuffer[]> captor = ArgumentCaptor.forClass(ByteBuffer[].class);
        verify(delegate).write(captor.capture(), eq(0), eq(BUFFERED_CHANNEL_BUFFER_SIZE));
        assertThat(lastElement(captor.getValue())).isSameAs(buffer1);
    }

    @Test
    public void should_buffer_when_buffer_range_added() throws Exception {
        BufferingChannel bufferingChannel = new BufferingChannel(delegate, 12);

        int bufferSize = 4;
        ByteBuffer buffer1 = ByteBuffer.allocate(bufferSize);
        ByteBuffer buffer2 = ByteBuffer.allocate(bufferSize);
        ByteBuffer buffer3 = ByteBuffer.allocate(bufferSize);
        ByteBuffer buffer4 = ByteBuffer.allocate(bufferSize);
        bufferingChannel.write(new ByteBuffer[]{buffer1, buffer2, buffer3, buffer4}, 1, 2);
        verifyNoMoreInteractions(delegate);

        assertThat(bufferingChannel.bufferIndex).isEqualTo(2);
        assertThat(bufferingChannel.buffer[0]).isSameAs(buffer2);
        assertThat(bufferingChannel.buffer[1]).isSameAs(buffer3);
    }

}