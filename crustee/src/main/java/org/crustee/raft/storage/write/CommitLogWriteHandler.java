package org.crustee.raft.storage.write;

import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.Segment;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class CommitLogWriteHandler implements EventHandler<WriteEvent>, LifecycleAware {

    private final CommitLog commitLog;

    public CommitLogWriteHandler(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        Segment newSegment = commitLog.write(event.getCommand());
        if (newSegment != null) {
            newSegment.acquire(); // this acquire is paired with a release by the last consumer of segments transmitted via events
            // avoid writing null in most cases, decreasing memory traffic is probably better than removing a correctly predicted branch
            // TODO microbench it !
            event.setSegment(newSegment);
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
