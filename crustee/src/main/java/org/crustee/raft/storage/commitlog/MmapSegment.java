package org.crustee.raft.storage.commitlog;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import org.crustee.raft.utils.ByteBufferUtils;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class MmapSegment implements Segment {

    private static final Logger logger = getLogger(MmapSegment.class);

    private UUID uuid = UUID.randomUUID();
    private final MappedByteBuffer mappedFile;
    private final Path file;
    private long syncedPosition = 0;

    private int referenceCount = 0;
    private volatile boolean closed = false;

    public MmapSegment(MappedByteBuffer buffer, Path file) {
        mappedFile = buffer;
        this.file = file;
    }

    public void append(ByteBuffer buffer, int size) {
        int remaining = buffer.remaining();
        for (int i = 0; i < remaining; i++) {
            mappedFile.put(buffer.get());
        }
    }

    public void append(ByteBuffer[] buffers, int length) {
        // to avoid some allocation, we allow to reuse the same array, so some slots may be null
        int size = 0;
        for (int i = 0; i < length; i++) {
            ByteBuffer buffer = buffers[i];
            append(buffer, size);
        }
    }

    public boolean canWrite(int size) {
        return mappedFile.remaining() >= size;
    }

    public long sync() {
        if (isSynced()) {
            return 0;
        }
        long bytesToSync = mappedFile.position() - syncedPosition;
        long positionAtSyncCall = mappedFile.position();
        mappedFile.force();
        syncedPosition = positionAtSyncCall;
        logger.trace("synced to {} for {}", syncedPosition, uuid);
        return bytesToSync;
    }

    public boolean isSynced() {
        return mappedFile.position() == syncedPosition;
    }

    public long getMaxSize() {
        return mappedFile.capacity();
    }

    public long getPosition() {
        return mappedFile.position();
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public synchronized void acquire() {
        if(closed) {
            throw new IllegalStateException("This segment is already closed");
        }
        referenceCount++;
    }

    @Override
    public synchronized void release() {
        referenceCount--;
        if(referenceCount == 0) {
            close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private void close() {
        logger.debug("Closing segment {}", this);
        closed = true;
        mappedFile.force(); // be sure that the segment is sync'ed to disk
        ByteBufferUtils.tryUnmap(mappedFile);
        UncheckedIOUtils.delete(file);
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            logger.warn("Segment was not closed before finalization");
            close();
        }
        super.finalize();
    }
}
