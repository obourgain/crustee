package org.crustee.raft.storage.write;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.crustee.raft.storage.commitlog.Segment;
import org.crustee.raft.storage.table.CrusteeTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MemtableHandlerTest {

    @Mock
    private CrusteeTable table;

    @Mock
    private Segment segment;

    @Test
    public void test_flush_when_threshold_is_exceeded() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS,
                new SynchronousQueue<>(),
                r -> {
                    // thread is started on first flush, so let's call countDown() here
                    latch.countDown();
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    t.setName(MemtableHandlerTest.class.getSimpleName());
                    return t;
                });

        MemtableHandler memtableHandler = new MemtableHandler(table, executor, segment, 1);

        try {
            memtableHandler.onEvent(dummyWriteEvent(), 1, false);
            boolean timelyRelease = latch.await(2, TimeUnit.SECONDS);
            assertThat(timelyRelease).isTrue();
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void should_register_new_segment_to_memtable() throws Exception {
        MemtableHandler handler = new MemtableHandler(table, newDirectExecutorService(), segment, 1);
        Segment newSegment = Mockito.mock(Segment.class);
        WriteEvent writeEvent = dummyWriteEvent();
        writeEvent.setSegment(newSegment);

        handler.onEvent(writeEvent, 10, true);

        handler.memtable.close(); // apply an action to the new segment
        verify(newSegment, times(2)).release(); // one when added to the event, one on close()
    }

    @Test
    public void should_register_new_memtable() throws Exception {
        new MemtableHandler(table, newDirectExecutorService(), segment, 1);
        verify(table).registerMemtable(any());
    }

    @Test
    public void should_register_sstable_after_flush() throws Exception {
        MemtableHandler memtableHandler = new MemtableHandler(table, newDirectExecutorService(), segment, 1);
        memtableHandler.onEvent(dummyWriteEvent(), 1, false);
        verify(table).memtableFlushed(any(), any());
    }

    protected WriteEvent dummyWriteEvent() {
        WriteEvent event = new WriteEvent();
        event.publish(ByteBuffer.allocate(4), ByteBuffer.allocate(4), emptyMap());
        return event;
    }
}