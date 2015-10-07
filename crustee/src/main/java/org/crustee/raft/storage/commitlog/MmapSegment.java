package org.crustee.raft.storage.commitlog;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.utils.ByteBufferUtils;
import org.slf4j.Logger;

public class MmapSegment implements Segment {

    private static final Logger logger = getLogger(MmapSegment.class);

    private final UUID uuid = UUID.randomUUID();
    private final MappedByteBuffer mappedFile;
    private final Path file;
    private long syncedPosition = 0;

    @VisibleForTesting
    protected int referenceCount = 0;
    private volatile boolean closed = false;

    public MmapSegment(MappedByteBuffer buffer, Path file) {
        mappedFile = buffer;
        this.file = file;
    }

    @Override
    public void append(ByteBuffer buffer) {
        int bytes = buffer.limit();
        for (int i = 0; i < bytes; i++) {
            mappedFile.put(buffer.get(i));
        }
    }

    @Override
    public boolean canWrite(int size) {
        return mappedFile.remaining() >= size;
    }

    @Override
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

    @Override
    public boolean isSynced() {
        return mappedFile.position() == syncedPosition;
    }

    @Override
    public long getMaxSize() {
        return mappedFile.capacity();
    }

    @Override
    public long getPosition() {
        return mappedFile.position();
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public synchronized void acquire() {
        if (closed) {
            acquires.forEach(Exception::printStackTrace);
            releases.forEach(Exception::printStackTrace);
            throw new IllegalStateException("This segment is already closed");
        }
        referenceCount++;
        acquires.add(new Exception());
    }

    private List<Exception> acquires = new ArrayList<>();
    private List<Exception> releases = new ArrayList<>();

    @Override
    public synchronized void release() {
        assert referenceCount > 0 : "released more than acquired";
        referenceCount--;
        releases.add(new Exception());
        if (referenceCount == 0) {
            close();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @VisibleForTesting
    protected void close() {
        logger.debug("Closing segment {}", this);
        closed = true;
        mappedFile.force(); // be sure that the segment is sync'ed to disk
        ByteBufferUtils.tryUnmap(mappedFile);
        // TODO have a segment manager to recycle/delete old segments
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            logger.warn("Segment was not closed before finalization");
//            acquires.forEach(Throwable::printStackTrace);
            close();
        }
        super.finalize();
    }

    @VisibleForTesting
    protected long getSyncedPosition() {
        return syncedPosition;
    }
}
