package org.crustee.raft.settings;

import static org.slf4j.LoggerFactory.getLogger;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;

/**
 * All these values must have a default value associated in {@link WellKnownSettings}
 */
public enum WellKnownSettings {

    RINGBUFFER_SIZE("ringbuffer.size", p -> p instanceof Number, () -> 1024),

    COMMITLOG_SEGMENT_SIZE("commitlog.segment.size", p -> p instanceof Number, () -> 1024),
    COMMITLOG_WRITER_MAX_EVENTS_BUFFER_SIZE("commitlog.writer.max.events.buffer.size", p -> p instanceof Number, () -> 1024 * 1024),

    COMMITLOG_WRITER_MAX_EVENTS_BUFFER_COUNT("commitlog.writer.max.events.buffer.count", p -> p instanceof Number, () -> 1024),
    COMMITLOG_WRITER_MAX_EVENTS_UNSYNCED_SIZE("commitlog.writer.max.events.unsynced.size", p -> p instanceof Number, () -> 1024 * 1024),

    COMMITLOG_WRITER_MAX_EVENTS_UNSYNCED_COUNT("commitlog.writer.max.events.unsynced.count", p -> p instanceof Number, () -> 1024),
    MEMTABLE_FLUSH_THREAD_MIN("memtable.flush.thread.min", p -> p instanceof Number, () -> 1),

    MEMTABLE_FLUSH_THREAD_MAX("memtable.flush.thread.max", p -> p instanceof Number, () -> 4);

    private static final Logger logger = getLogger(WellKnownSettings.class);

    private final String propertyName;
    private final Predicate<Object> validator;
    private final Supplier<?> defaultValue;

    <T> WellKnownSettings(String propertyName, Predicate<Object> validator, Supplier<T> defaultValue) {
        this.propertyName = propertyName;
        this.validator = validator;
        this.defaultValue = defaultValue;
    }

    public <T> T apply(Map<String, Object> map) {
        Object value = map.get(propertyName);
        if(value == null) {
            value = defaultValue.get();
            logger.trace("Using default value {} for {}", value, propertyName);
        }
        validate(value);
        return (T) value;
    }

    public void validate(Object value) {
        if (!validator.test(value)) {
            throw new IllegalArgumentException("value " + value + " is not valid for " + propertyName);
        }
    }
}
