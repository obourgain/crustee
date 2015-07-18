package org.crustee.raft.storage.commitlog;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.crustee.raft.utils.UncheckedIOUtils.fsyncDir;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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

    private static final boolean USE_MMAP_SEGMENT = true;

    private final int size;
    private final ExecutorService executor;

    public SegmentFactory(int size) {
        this.size = size;
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
        try {
            if (USE_MMAP_SEGMENT) {
                Path path = Files.createTempFile(null, null);
                fsyncDir(path.getParent());
                try(FileChannel fileChannel = UncheckedIOUtils.openChannel(path, READ, WRITE)) {
                    MappedByteBuffer map = fileChannel.map(READ_WRITE, 0, size);
                    return new MmapSegment(map);
                }
            } else {
                // TODO gather metrics on segment creation
                Path path = Files.createTempFile(null, null);
                RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
                file.setLength(size);
                // file length is in metadata
                file.getChannel().force(true);
                ChannelSegment segment = new ChannelSegment(file.getChannel());
                fsyncDir(path.getParent());
                return segment;

            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
