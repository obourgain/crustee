package org.crustee.raft.misc;

import static org.slf4j.LoggerFactory.getLogger;
import java.lang.management.ManagementFactory;
import org.slf4j.Logger;

public class AllocationTracker {
    private static final Logger logger = getLogger(AllocationTracker.class);

    private static final long GARBAGE_GENERATED_BY_MEASURE;

    private com.sun.management.ThreadMXBean threadMXBean;
    private long allocatedAtStartTracking = -1;
    private long recorded = 0;
    private long threadId;

    static {
        AllocationTracker tracker = new AllocationTracker();
        long sum = 0;
        for (int i = 0; i < 10; i++) {
            tracker.startRecording();
            long recording = tracker.stopRecording();
            sum += recording;
        }
        GARBAGE_GENERATED_BY_MEASURE = sum / 10;
        logger.debug("Allocation tracking generated {} bytes of garbage per measure", GARBAGE_GENERATED_BY_MEASURE);
    }

    public AllocationTracker() {
        threadId = Thread.currentThread().getId();

        try {
            threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        } catch (Exception e) {
            if(logger.isDebugEnabled()) {
                logger.warn("Per thread Allocation tracking is not available {}", e);
            } else{
                logger.warn("Per thread Allocation tracking is not available {}", e.getMessage());
            }
        }
    }

    public void reset() {
        checkOwnerThread();
        allocatedAtStartTracking = -1;
        recorded = 0;
    }

    /**
     * @return the total number of bytes allocated by the current thread for every {@link #startRecording()} / {@link #stopRecording()} pairs of calls
     */
    public long getTotalRecorded() {
        return recorded;
    }

    /**
     * @return the number of bytes allocated by the current thread since the last call to {@link #startRecording()}
     */
    public long stopRecording() {
        checkOwnerThread();
        assert allocatedAtStartTracking != -1 : "start must be called before";
        long recordedForThisRun = threadMXBean.getThreadAllocatedBytes(threadId) - allocatedAtStartTracking - GARBAGE_GENERATED_BY_MEASURE;
        recorded += recordedForThisRun;
        return recordedForThisRun;
    }

    public AllocationTracker startRecording() {
        checkOwnerThread();
        allocatedAtStartTracking = threadMXBean.getThreadAllocatedBytes(threadId);
        return this;
    }

    private void checkOwnerThread() {
        assert threadId == Thread.currentThread().getId() : "AllocationTracker should not be shared between threads";
    }


}