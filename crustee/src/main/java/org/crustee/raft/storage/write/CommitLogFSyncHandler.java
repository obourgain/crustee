package org.crustee.raft.storage.write;

import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import com.lmax.disruptor.EventHandler;

public class CommitLogFSyncHandler implements EventHandler<WriteEvent> {

    private final CommitLog commitLog;
    private Segment current;

    private final int maxUnsyncedSizeInBytes;
    private final int maxUnsyncedCount;
    private int eventCount = 0;
    private int unsyncedSizeInBytes = 0;

    public CommitLogFSyncHandler(CommitLog commitLog, int maxUncommitedSize, int maxUnsyncedCount) {
        this.commitLog = commitLog;
        this.current = commitLog.getCurrentSegment();
        this.maxUnsyncedSizeInBytes = maxUncommitedSize;
        this.maxUnsyncedCount = maxUnsyncedCount;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        if(event.getSegment() != null) {
            current = event.getSegment();
        }
        eventCount++;
        unsyncedSizeInBytes += event.getRowKey().limit(); // + event.getValues().limit(); TODO log the value
        syncOldSegments();
        syncCurrentIfNeeded(endOfBatch);
    }

    private void syncCurrentIfNeeded(boolean endOfBatch) {
        if (endOfBatch || unsyncedSizeInBytes >= maxUnsyncedSizeInBytes || eventCount >= maxUnsyncedCount) {
            current.sync();
            eventCount = 0;
            unsyncedSizeInBytes = 0;
        }
    }

    private void syncOldSegments() {
        Segment old;
        while ((old = commitLog.getOldSegments().poll()) != null) {
            unsyncedSizeInBytes -= old.sync();
        }
    }

}
