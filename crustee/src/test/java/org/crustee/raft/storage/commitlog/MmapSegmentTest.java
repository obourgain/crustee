package org.crustee.raft.storage.commitlog;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import org.crustee.raft.utils.ByteBufferUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MmapSegmentTest {

    private static final int SEGMENT_SIZE = 10 * 1024;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path path;
    private MappedByteBuffer mmap;

    @Before
    public void create_file_and_mmap() throws IOException {
        File file = temporaryFolder.newFile();
        path = file.toPath();
        try (RandomAccessFile raf = UncheckedIOUtils.openRandomAccessFile(path, "rw")) {
            raf.setLength(SEGMENT_SIZE);
        }
        mmap = UncheckedIOUtils.map(path);
    }

    @After
    public void cleanup() throws IOException {
        if (mmap != null) {
            ByteBufferUtils.tryUnmap(mmap);
        }
    }

    @Test
    public void should_append_buffer() throws Exception {
        MmapSegment segment = new MmapSegment(mmap, path);
        try {
            int size = 12;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            segment.append(buffer);

            assertThat(segment.getMaxSize()).isEqualTo(SEGMENT_SIZE);
            assertThat(segment.getPosition()).isEqualTo(size);
            assertThat(buffer.position()).describedAs("should not consume the buffer").isEqualTo(0);
        } finally {
            segment.close();
        }
    }

    @Test
    public void should_sync_if_needed() throws Exception {
        MmapSegment segment = new MmapSegment(mmap, path);
        try {
            int size = 12;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            segment.append(buffer);
            assertThat(segment.isSynced()).isFalse();
            assertThat(segment.getSyncedPosition()).isEqualTo(0);

            segment.sync();
            assertThat(segment.getSyncedPosition()).isEqualTo(size);
            assertThat(segment.isSynced()).isTrue();
        } finally {
            segment.close();
        }
    }

    @Test
    public void should_determine_if_can_accept_buffer() throws Exception {
        MmapSegment segment = new MmapSegment(mmap, path);

        try {
            assertThat(segment.canWrite(10)).isTrue();

            ByteBuffer fill = ByteBuffer.allocate(SEGMENT_SIZE - 2);
            segment.append(fill);
            assertThat(segment.canWrite(10)).isFalse();
        } finally {
            segment.close();
        }
    }

    @Test
    public void should_increment_decrement_acquires_count() throws Exception {
        MmapSegment segment = new MmapSegment(mmap, path);

        segment.acquire();
        assertThat(segment.referenceCount).isEqualTo(1);
        segment.release();
        assertThat(segment.referenceCount).isEqualTo(0);
    }

    @Test
    public void should_close_when_ref_count_is_zero() throws Exception {
        MmapSegment segment = new MmapSegment(mmap, path);

        segment.acquire();
        assertThat(segment.referenceCount).isEqualTo(1);
        segment.release();
        assertThat(segment.referenceCount).isEqualTo(0);
        assertThat(segment.isClosed()).isTrue();
    }

}