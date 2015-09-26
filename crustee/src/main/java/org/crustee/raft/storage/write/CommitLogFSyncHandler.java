package org.crustee.raft.storage.write;

import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import com.lmax.disruptor.EventHandler;

public class CommitLogFSyncHandler implements EventHandler<WriteEvent> {

    private final CommitLog commitLog;

    private final int maxUnsyncedSizeInBytes;
    private final int maxUnsyncedCount;
    private int eventCount = 0;
    private int unsyncedSizeInBytes = 0;

    public CommitLogFSyncHandler(CommitLog commitLog, int maxUncommitedSize, int maxUnsyncedCount) {
        this.commitLog = commitLog;
        this.maxUnsyncedSizeInBytes = maxUncommitedSize;
        this.maxUnsyncedCount = maxUnsyncedCount;
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
        if(endOfBatch || eventCount >= maxUnsyncedCount || unsyncedSizeInBytes >= maxUnsyncedSizeInBytes) {
            commitLog.getCurrent().sync();
            eventCount = 0;
            unsyncedSizeInBytes = 0;
        }
    }

}
