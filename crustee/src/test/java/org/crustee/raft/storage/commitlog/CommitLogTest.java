package org.crustee.raft.storage.commitlog;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class CommitLogTest {

    @Test
    public void should_write_in_current_buffer() throws Exception {
        SegmentFactory segmentFactory = new SegmentFactory(100);
        CommitLog commitLog = new CommitLog(segmentFactory);
        Segment firstSegment = commitLog.getCurrentSegment();
        firstSegment.acquire();

        try {
            ByteBuffer buffer = ByteBuffer.allocate(10);
            Segment newSegment = commitLog.write(buffer);
            assertThat(newSegment).isNull();
            assertThat(commitLog.getCurrentSegment().getPosition()).isEqualTo(buffer.limit());
        } finally {
            commitLog.getCurrentSegment().release();
        }
    }

    @Test
    public void should_write_in_next_buffer_if_not_enough_room() throws Exception {
        SegmentFactory segmentFactory = new SegmentFactory(15);
        CommitLog commitLog = new CommitLog(segmentFactory);
        Segment firstSegment = commitLog.getCurrentSegment();
        firstSegment.acquire();

        try {
            int bufferSize = 10;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            Segment newSegment = commitLog.write(buffer);
            assertThat(newSegment).isNull();

            buffer = ByteBuffer.allocate(bufferSize);
            newSegment = commitLog.write(buffer);
            assertThat(newSegment).isNotNull();

            assertThat(commitLog.getCurrentSegment().getPosition()).isEqualTo(buffer.limit());
            assertThat(commitLog.syncOldSegments()).isEqualTo(bufferSize);
        } finally {
            commitLog.getCurrentSegment().release();
        }
    }

    @Test
    public void should_sync_segments() throws Exception {
        SegmentFactory segmentFactory = new SegmentFactory(15);
        CommitLog commitLog = new CommitLog(segmentFactory);
        Segment firstSegment = commitLog.getCurrentSegment();
        firstSegment.acquire();
        try {

            int bufferSize = 10;
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            commitLog.write(buffer);

            buffer = ByteBuffer.allocate(bufferSize);
            commitLog.write(buffer);

            long synced = commitLog.syncSegments();
            Assertions.assertThat(synced).isEqualTo(2 * bufferSize);

            assertThat(firstSegment.isSynced()).isTrue();
            assertThat(commitLog.getCurrentSegment().isSynced()).isTrue();
        } finally {
            commitLog.getCurrentSegment().release();
        }
    }

}