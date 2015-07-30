package org.crustee.raft.storage.row;

import static java.nio.ByteBuffer.allocate;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.data.MapEntry;
import org.junit.Test;

public class MapRowTest {

    @Test
    public void should_be_constructed_correctly() throws Exception {
        MapRow row = new MapRow(singletonMap(allocate(42), allocate(2048)));

        assertThat(row.getEstimatedSizeInBytes()).isEqualTo(42 + 2048);
        assertThat(row.asMap()).isEqualTo(singletonMap(allocate(42), allocate(2048)));
    }

    @Test
    public void should_be_overwritten_correctly() throws Exception {
        MapRow row = new MapRow(singletonMap(allocate(42), allocate(2048)));

        // same empty buffer of same size as key
        row.addAll(singletonMap(allocate(42), allocate(1024)));
        assertThat(row.getEstimatedSizeInBytes()).isEqualTo(42 + 1024);
        assertThat(row.asMap()).isEqualTo(singletonMap(allocate(42), allocate(1024)));
    }

    @Test
    public void should_be_merged_correctly() throws Exception {
        MapRow row = new MapRow(singletonMap(allocate(42), allocate(2048)));

        // key size is different
        row.addAll(singletonMap(allocate(43), allocate(1024)));
        assertThat(row.getEstimatedSizeInBytes()).isEqualTo(42 + 2048 + 43 + 1024);
        assertThat(row.asMap()).contains(MapEntry.entry(allocate(42), allocate(2048)), MapEntry.entry(allocate(43), allocate(1024)));
    }
}