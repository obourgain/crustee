package org.crustee.raft.storage.commitlog;

import static org.crustee.raft.utils.UncheckedIOUtils.fsyncDir;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class SegmentFactory {

    private static final Logger logger = getLogger(SegmentFactory.class);

    private final int size;
    private final ExecutorService executor;

    public SegmentFactory(int segmentSize) {
        this.size = segmentSize;
        this.executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>(1), r -> {
            Thread thread = new Thread(r);
            thread.setName("segment-factory-thread");
            thread.setUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception in " + t.getName(), e));
            return thread;
        });
    }

    public Future<Segment> newSegment() {
        return executor.submit(this::createSegment);
    }

    protected Segment createSegment() {
        Path path = UncheckedIOUtils.tempFile();
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(size);
            // file length is in metadata
            file.getChannel().force(true);
            fsyncDir(path.getParent());
            MappedByteBuffer map = UncheckedIOUtils.mapReadWrite(path);
            return new MmapSegment(map, path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
