package org.crustee.raft.storage.write;

import static java.util.Collections.singleton;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import org.junit.Test;

public class CommitLogFSyncHandlerTest {

    @Test
    public void should_buffer_syncs() throws Exception {
        CommitLog commitLog = mock(CommitLog.class);
        when(commitLog.getOldSegments()).thenReturn(new LinkedBlockingQueue<>());
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, false);

        verify(commitLog, never()).syncCurrent();
    }

    @Test
    public void should_sync_at_end_of_batch() throws Exception {
        CommitLog commitLog = mock(CommitLog.class);
        when(commitLog.getOldSegments()).thenReturn(new LinkedBlockingQueue<>());
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, true);

        verify(commitLog).syncCurrent();
    }

    @Test
    public void should_sync_old_segments() throws Exception {
        CommitLog commitLog = mock(CommitLog.class);
        Segment segment = mock(Segment.class);
        when(commitLog.getOldSegments()).thenReturn(new LinkedBlockingQueue<>(singleton(segment)));
        CommitLogFSyncHandler handler = new CommitLogFSyncHandler(commitLog, 100, 100);

        WriteEvent event = mock(WriteEvent.class);
        when(event.getRowKey()).thenReturn(ByteBuffer.allocate(12));
        handler.onEvent(event, 10, true);

        verify(segment).sync();
    }

}