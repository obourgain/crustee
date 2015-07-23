package org.crustee.raft.storage.write;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.slf4j.Logger;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class CommitLogWriteHandler implements EventHandler<WriteEvent>, LifecycleAware {

    private static final Logger logger = getLogger(CommitLogWriteHandler.class);

    private final CommitLog commitLog;

    private final int maxEvents = 1000;
    private final int maxSizeInBytes = 1024 * 1024;

    private int sizeInBytes = 0;
    private final ByteBuffer[] buffered = new ByteBuffer[maxEvents];
    private int nextBufferIndex = 0;

    public CommitLogWriteHandler(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        buffered[nextBufferIndex] = event.getCommand();
        nextBufferIndex++;
        sizeInBytes += event.getCommand().limit();
        // it is safe to buffer stuff because we won't publish the next sequence and thus allow
        // the next consumer to catch up until we receive a endOfBatch = true
        if (endOfBatch || nextBufferIndex >= maxEvents || sizeInBytes >= maxSizeInBytes) {
            commitLog.write(buffered, nextBufferIndex);
            Arrays.fill(buffered, null);
            nextBufferIndex = 0;
            sizeInBytes = 0;
        }

        if (sequence % 10_000_000 == 0) {
            logger.info("journaled event {}", sequence);
        }
    }

    @Override
    public void onStart() {
        commitLog.setOwnerThread();
    }

    @Override
    public void onShutdown() {

    }
}
