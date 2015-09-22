package org.crustee.raft.storage.commitlog;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import org.crustee.raft.storage.btree.ArrayUtils;
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
    }

    public void write(ByteBuffer buffer, int length) {
        checkOwnerThread();
        if(!current.canWrite(length)) {
            oldSegments.add(current);
            current = UncheckedFutureUtils.get(next);
            next = segmentFactory.newSegment();
        }
        current.append(buffer, length);
    }

    public void write(ByteBuffer[] buffers, int length) {
        checkOwnerThread();
        int sizeInBytes = buffersSize(buffers, length);
        // TODO write as much a possible in the current log
        if(!current.canWrite(sizeInBytes)) {
            oldSegments.add(current);
            current = UncheckedFutureUtils.get(next);
            next = segmentFactory.newSegment();
        }
        current.append(buffers, length);
    }

    protected static int buffersSize(ByteBuffer[] buffers, int length) {
        assert ArrayUtils.lastNonNullElementIndex(buffers) + 1 == length : "off by one error " + Arrays.toString(buffers) + " / " + length;
        long size = 0;
        for (int i = 0; i < length; i++) {
            size += buffers[i].limit();
        }
        // we don't want to write a big chunk at once in the log
        assert size <= Integer.MAX_VALUE: "int overflow: " + size;
        return (int) size;
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

    public Queue<Segment> getOldSegments() {
        return oldSegments;
    }
}
