package org.crustee.raft.settings;

import static org.crustee.raft.settings.WellKnownSettings.*;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.slf4j.Logger;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

public class Settings {

    private static final Logger logger = getLogger(Settings.class);

    private final int ringbufferSize;
    private final int commitLogSegmentSize;
    private final int commitlogWriterMaxEventsBufferSize;
    private final int commitlogWriterMaxEventsBufferCount;
    private final int commitlogWriterMaxEventsUnsyncedSize;
    private final int commitlogWriterMaxEventsUnsyncedCount;
    private final int memtableFlushThreadMin;
    private final int memtableFlushThreadMax;

    public Settings(Path path) {
        logger.info("loading settings from {}", path);
        Preconditions.checkState(path != null, "expected non null source %s", path);
        Preconditions.checkState(Files.exists(path), "expected source to exist %s", path);
        Preconditions.checkState(Files.isReadable(path), "expected source to be readable %s", path);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map<String, Object> map = doRead(path, mapper);

        ringbufferSize = RINGBUFFER_SIZE.apply(map);
        commitLogSegmentSize = COMMITLOG_SEGMENT_SIZE.apply(map);

        commitlogWriterMaxEventsBufferSize = COMMITLOG_WRITER_MAX_EVENTS_BUFFER_SIZE.apply(map);
        commitlogWriterMaxEventsBufferCount = COMMITLOG_WRITER_MAX_EVENTS_BUFFER_COUNT.apply(map);

        commitlogWriterMaxEventsUnsyncedSize = COMMITLOG_WRITER_MAX_EVENTS_UNSYNCED_SIZE.apply(map);
        commitlogWriterMaxEventsUnsyncedCount = COMMITLOG_WRITER_MAX_EVENTS_UNSYNCED_COUNT.apply(map);

        memtableFlushThreadMin = MEMTABLE_FLUSH_THREAD_MIN.apply(map);
        memtableFlushThreadMax = MEMTABLE_FLUSH_THREAD_MAX.apply(map);

        // remember to update toString() when adding fields
        logger.debug("loaded settings {}", this);
    }

    private Map<String, Object> doRead(Path path, ObjectMapper mapper) {
        try {
            return mapper.readValue(path.toFile(), new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new IllegalStateException("unable to load config from " + path);
        }
    }

    public int getRingbufferSize() {
        return ringbufferSize;
    }

    public int getCommitLogSegmentSize() {
        return commitLogSegmentSize;
    }

    public int getCommitlogWriterMaxEventsBufferSize() {
        return commitlogWriterMaxEventsBufferSize;
    }

    public int getCommitlogWriterMaxEventsBufferCount() {
        return commitlogWriterMaxEventsBufferCount;
    }

    public int getCommitlogWriterMaxEventsUnsyncedSize() {
        return commitlogWriterMaxEventsUnsyncedSize;
    }

    public int getCommitlogWriterMaxEventsUnsyncedCount() {
        return commitlogWriterMaxEventsUnsyncedCount;
    }

    public int getMemtableFlushThreadMin() {
        return memtableFlushThreadMin;
    }

    public int getMemtableFlushThreadMax() {
        return memtableFlushThreadMax;
    }

    @Override
    public String toString() {
        return "Settings{" +
                "ringbufferSize=" + ringbufferSize +
                ", commitLogSegmentSize=" + commitLogSegmentSize +
                ", commitlogWriterMaxEventsBufferSize=" + commitlogWriterMaxEventsBufferSize +
                ", commitlogWriterMaxEventsBufferCount=" + commitlogWriterMaxEventsBufferCount +
                ", commitlogWriterMaxEventsUnsyncedSize=" + commitlogWriterMaxEventsUnsyncedSize +
                ", commitlogWriterMaxEventsUnsyncedCount=" + commitlogWriterMaxEventsUnsyncedCount +
                ", memtableFlushThreadMin=" + memtableFlushThreadMin +
                ", memtableFlushThreadMax=" + memtableFlushThreadMax +
                '}';
    }
}
