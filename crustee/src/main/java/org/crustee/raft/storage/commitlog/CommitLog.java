package org.crustee.raft.storage.commitlog;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import org.crustee.raft.utils.UncheckedFutureUtils;

public class CommitLog {

    private Segment current;
    private Future<Segment> next;
    private Queue<Segment> oldSegments = new ConcurrentLinkedQueue<>();

    private WeakReference<Thread> owner;

    private final SegmentFactory segmentFactory;

    public CommitLog(SegmentFactory segmentFactory) {
        this.segmentFactory = segmentFactory;
        this.current = UncheckedFutureUtils.get(segmentFactory.newSegment());
        this.next = segmentFactory.newSegment();
        setOwnerThread();
    }

    public Segment write(ByteBuffer buffer) {
        checkOwnerThread();
        assert buffer.position() == 0;
        int sizeInBytes = buffer.limit();
        // TODO write as much a possible in the current log
        if (current.canWrite(sizeInBytes)) {
            current.append(buffer);
            return null;
        } else {
            oldSegments.add(current);
            current = UncheckedFutureUtils.get(next);
            current.acquire(); // this acquire must be paired with a release when the old segment is fsync'ed
            next = segmentFactory.newSegment();
            current.append(buffer);
            return current;
        }
    }

    private void checkOwnerThread() {
        assert owner.get() == Thread.currentThread() : "expected owner thread to be " + owner.get() + " but it was " + Thread.currentThread();
    }

    public void setOwnerThread() {
        if(CommitLog.class.desiredAssertionStatus()) {
            owner = new WeakReference<>(Thread.currentThread());
        }
    }

    public long syncCurrent() {
        return current.sync();
    }

    public Segment getCurrentSegment() {
        return current;
    }

    public long syncOldSegments() {
        Segment old;
        long synced = 0;
        while ((old = oldSegments.poll()) != null) {
            synced += old.sync();
            old.release();
        }
        return synced;
    }

    public long syncSegments() {
        long synced = syncOldSegments();
        // TODO there is a race confition here if the current changes and is put in oldSegents, we won't sync it
        // next sync call will flush it but i am not really comfortable with having an old commit log unsynced while
        // the current one is synced
        synced += syncCurrent();
        return synced;
    }
}
