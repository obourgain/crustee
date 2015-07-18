package org.crustee.raft.storage.commitlog;

import static org.crustee.raft.utils.UncheckedIOUtils.setPosition;
import static org.crustee.raft.utils.UncheckedIOUtils.size;
import static org.crustee.raft.utils.UncheckedIOUtils.write;
import static org.slf4j.LoggerFactory.getLogger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.UUID;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;

public class ChannelSegment implements Segment {

    private static final Logger logger = getLogger(ChannelSegment.class);

    private UUID uuid = UUID.randomUUID();
    private final FileChannel channel;
    private long position;
    private long channelSize;
    private long syncedPosition = 0;
    private volatile boolean closed = false;

    public ChannelSegment(FileChannel channel) {
        this.channel = channel;
        this.position = 0;
        this.channelSize = size(channel);
    }

    public void append(ByteBuffer buffer, int size) {
        write(channel, buffer, position);
        position += size;
    }

    public void append(ByteBuffer[] buffers, int length) {
        // to avoid some allocation, we allow to reuse the same array, so some slots may be null
        int size = 0;
        for (int i = 0; i < length; i++) {
            ByteBuffer buffer = buffers[i];
            size += buffer.limit();
        }
        setPosition(channel, position); // we can not write a bunch of buffers at an arbitrary position
        write(channel, buffers);
        position += size;
        // release cached array
        Arrays.fill(buffers, null);
    }

    public boolean canWrite(int size) {
        return position + size <= this.channelSize;
    }

    public long sync() {
        if (isSynced()) {
            return 0;
        }
        try {
            long bytesToSync = position - syncedPosition;
            long positionAtSyncCall = position;
            channel.force(false);
            syncedPosition = positionAtSyncCall;
            logger.trace("synced to {} for {}", syncedPosition, uuid);
            return bytesToSync;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isSynced() {
        return position == syncedPosition;
    }

    public long getMaxSize() {
        return channelSize;
    }

    public long getPosition() {
        return position;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public void close() {
        UncheckedIOUtils.close(channel);
        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(!closed) {
            logger.warn("Segment was not closed before finalization");
            close();
        }
    }
}
