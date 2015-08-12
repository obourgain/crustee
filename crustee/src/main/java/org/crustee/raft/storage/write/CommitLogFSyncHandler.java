package org.crustee.raft.storage.write;

import static org.slf4j.LoggerFactory.getLogger;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import org.slf4j.Logger;
import com.lmax.disruptor.EventHandler;

public class CommitLogFSyncHandler implements EventHandler<WriteEvent> {

    private static final Logger logger = getLogger(CommitLogFSyncHandler.class);

    private final CommitLog commitLog;

    private final int maxEvents = 1000;

    private final int maxUnsyncedSizeInBytes;
    private int eventCount = 0;
    private int unsyncedSizeInBytes = 0;

    public CommitLogFSyncHandler(CommitLog commitLog, int maxUncommitedSize, int maxUnsyncedCount) {
        this.commitLog = commitLog;
        this.maxUnsyncedSizeInBytes = maxUncommitedSize;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        eventCount++;
        unsyncedSizeInBytes += event.getRowKey().limit(); // + event.getValues().limit(); TODO log the value
        Segment old;
        // sync all old commit logs
        while ((old = commitLog.getOldSegments().poll()) != null) {
            old.sync();
        }
        if(endOfBatch || eventCount >= maxEvents || unsyncedSizeInBytes >= maxUnsyncedSizeInBytes) {
            commitLog.getCurrent().sync();
        }
    }

}
