package org.crustee.raft.storage.commitlog;

import java.lang.ref.WeakReference;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.crustee.raft.utils.UncheckedFutureUtils;

public class CommitLog {

    private Segment current;
    private Future<Segment> next;
    private BlockingQueue<Segment> oldSegments = new LinkedBlockingQueue<>();

    private WeakReference<Thread> owner;

    private final SegmentFactory segmentFactory;

    public CommitLog(SegmentFactory segmentFactory) {
        this.segmentFactory = segmentFactory;
        this.current = UncheckedFutureUtils.get(segmentFactory.newSegment());
        this.next = segmentFactory.newSegment();
    }

    public void write(ByteBuffer buffer, int size) {
        checkOwnerThread();
        if(!current.canWrite(size)) {
            oldSegments.add(current);
            current = UncheckedFutureUtils.get(next);
            next = segmentFactory.newSegment();
        }
        current.append(buffer, size);
    }

    public void write(ByteBuffer[] buffers, int length) {
        checkOwnerThread();
        int sizeInBytes = Arrays.stream(buffers).filter(e -> e != null).mapToInt(Buffer::limit).sum();
        // TODO write as much a possible in the current log
        if(!current.canWrite(sizeInBytes)) {
            oldSegments.add(current);
            current = UncheckedFutureUtils.get(next);
            next = segmentFactory.newSegment();
        }
        current.append(buffers, length);
    }

    private void checkOwnerThread() {
        assert owner.get() == Thread.currentThread() : "expected owner thread to be " + owner.get() + " but it was " + Thread.currentThread();
    }

    public void setOwnerThread() {
        owner = new WeakReference<>(Thread.currentThread());
    }

    public Segment getCurrent() {
        return current;
    }

    public BlockingQueue<Segment> getOldSegments() {
        return oldSegments;
    }
}
