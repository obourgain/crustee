package org.crustee.raft.storage.commitlog;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

public class SegmentFactoryTest {

    @Test
    public void should_create_new_segment() throws Exception {
        SegmentFactory factory = new SegmentFactory(128);
        Segment segment = factory.createSegment();
        segment.acquire();
        assertThat(segment.getMaxSize()).isEqualTo(128);
        assertThat(segment.getPosition()).isEqualTo(0);
        assertThat(segment.isSynced()).isEqualTo(true);
        segment.release();
    }

    @Test
    public void should_ref_count_segments() throws Exception {
        SegmentFactory factory = new SegmentFactory(128);
        Segment segment = factory.createSegment();
        segment.acquire();
        assertThat(segment.isClosed()).isFalse();
        segment.release();
        assertThat(segment.isClosed()).isTrue();
    }
}