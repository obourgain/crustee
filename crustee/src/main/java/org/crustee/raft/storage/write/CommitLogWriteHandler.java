package org.crustee.raft.storage.write;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import org.slf4j.Logger;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class CommitLogWriteHandler implements EventHandler<WriteEvent>, LifecycleAware {

    private static final Logger logger = getLogger(CommitLogWriteHandler.class);

    private final CommitLog commitLog;

    private final int maxEvents;
    private final int maxSizeInBytes;

    private int sizeInBytes = 0;
    private final ByteBuffer[] buffered;
    private int nextBufferIndex = 0;

    public CommitLogWriteHandler(CommitLog commitLog, int maxSizeInBytes, int maxEvents) {
        this.commitLog = commitLog;
        this.maxSizeInBytes = maxSizeInBytes;
        this.maxEvents = maxEvents;
        buffered = new ByteBuffer[this.maxEvents];
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        buffered[nextBufferIndex] = event.getCommand();
        nextBufferIndex++;
        sizeInBytes += event.getCommand().limit();
        // it is safe to buffer stuff because we won't publish the next sequence and thus allow
        // the next consumer to catch up until we receive a endOfBatch = true
        if (endOfBatch || nextBufferIndex >= maxEvents || sizeInBytes >= maxSizeInBytes) {
            flushBuffer(event);
        }
    }

    private void flushBuffer(WriteEvent event) {
        // newSegment is null except when the segment changed
        Segment newSegment = commitLog.write(buffered, nextBufferIndex);
        if(newSegment != null) {
            // avoid writing null in most cases, decreasing memory traffic is probably better than removing a correctly predicted branch
            // TODO microbench it !
            newSegment.acquire();
            event.setSegment(newSegment);
        }

        Arrays.fill(buffered, null);
        nextBufferIndex = 0;
        sizeInBytes = 0;
    }

    @Override
    public void onStart() {
        commitLog.setOwnerThread();
    }

    @Override
    public void onShutdown() {

    }
}
