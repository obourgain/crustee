package org.crustee.raft.storage.write;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommitLogFSyncHandlerTest {

    @Mock
    CommitLog commitLog;
    @Mock
    Segment segment;

    @Test
    public void should_buffer_syncs() throws Exception {
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, false);

        verify(commitLog, never()).syncCurrent();
    }

    @Test
    public void should_sync_at_end_of_batch() throws Exception {
        when(commitLog.getCurrentSegment()).thenReturn(segment);
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, true);

        verify(segment).sync();
    }

    @Test
    public void should_sync_old_segments() throws Exception {
        when(commitLog.getCurrentSegment()).thenReturn(segment);
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, true);

        verify(commitLog).syncSegments();
    }

}
