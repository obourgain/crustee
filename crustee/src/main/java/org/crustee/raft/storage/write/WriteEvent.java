package org.crustee.raft.storage.write;

import java.nio.ByteBuffer;
import java.util.Map;
import org.crustee.raft.storage.commitlog.Segment;

public class WriteEvent {

    private ByteBuffer rowKey;
    private ByteBuffer command;
    private Map values;

    // this is non null only when this entry was written in a new segment
    private Segment segment;

    public ByteBuffer getRowKey() {
        return rowKey;
    }

    public Map getValues() {
        return values;
    }

    public ByteBuffer getCommand() {
        return command;
    }

    public void setSegment(Segment segment) {
        this.segment = segment;
    }

    public void publish(ByteBuffer command, ByteBuffer rowKey, Map values) {
        this.command = command;
        this.rowKey = rowKey;
        this.values = values;
        this.segment = null;
    }

    public Segment getSegment() {
        return segment;
    }
}
