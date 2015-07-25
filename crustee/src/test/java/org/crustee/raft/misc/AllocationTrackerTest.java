package org.crustee.raft.misc;

import java.util.ArrayList;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class AllocationTrackerTest {

    @Test
    public void should_return_zero_when_not_allocating() throws Exception {
        AllocationTracker tracker = new AllocationTracker();
        tracker.startRecording();
        long allocs = tracker.stopRecording();

        Assertions.assertThat(allocs).isEqualTo(0);
    }

    @Test
    public void should_measure_garbage() throws Exception {
        AllocationTracker tracker = new AllocationTracker();
        tracker.startRecording();
        new ArrayList<>();
        long allocs = tracker.stopRecording();

        Assertions.assertThat(allocs).isGreaterThan(10);
        Assertions.assertThat(tracker.getTotalRecorded()).isEqualTo(allocs);
    }

    @Test
    public void should_stop_restart_measure() throws Exception {
        AllocationTracker tracker = new AllocationTracker();
        tracker.startRecording();
        new ArrayList<>();
        long firstMeasureAllocs = tracker.stopRecording();
        tracker.startRecording();
        new ArrayList<>();
        long secondMeasureAllocs = tracker.stopRecording();

        Assertions.assertThat(secondMeasureAllocs).isEqualTo(firstMeasureAllocs);
        Assertions.assertThat(tracker.getTotalRecorded()).isEqualTo(firstMeasureAllocs + secondMeasureAllocs);
    }

    @Test
    public void should_reset() throws Exception {
        AllocationTracker tracker = new AllocationTracker();
        tracker.startRecording();
        new ArrayList<>();
        tracker.stopRecording();

        tracker.reset();

        Assertions.assertThat(tracker.getTotalRecorded()).isEqualTo(0);
    }
}