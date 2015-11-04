package org.crustee.raft.storage.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommitLogWriteHandlerTest {

    @Mock
    CommitLog commitLog;
    @Mock
    Segment segment;

    @Test
    public void should_write_in_commit_log() throws Exception {
        CommitLogWriteHandler handler = new CommitLogWriteHandler(commitLog);

        WriteEvent event = new WriteEvent();
        ByteBuffer command = ByteBuffer.allocate(10);
        event.publish(command, ByteBuffer.allocate(4), Collections.emptyMap());
        handler.onEvent(event, 1, false);

        verify(commitLog).write(command);
        assertThat(event.getSegment()).isNull();
    }

    @Test
    public void should_add_new_comit_log_segment_in_event() throws Exception {
        CommitLogWriteHandler handler = new CommitLogWriteHandler(commitLog);
        when(commitLog.write(any())).thenReturn(segment);

        WriteEvent event = new WriteEvent();
        ByteBuffer command = ByteBuffer.allocate(10);
        event.publish(command, ByteBuffer.allocate(4), Collections.emptyMap());
        handler.onEvent(event, 1, false);

        assertThat(event.getSegment()).isNotNull();
        verify(segment).acquire();
    }
}