package org.crustee.raft.storage.write;

import java.nio.ByteBuffer;
import java.util.Map;

public class WriteEvent {

    private ByteBuffer rowKey;
    private ByteBuffer command;
    private Map values;

    public ByteBuffer getRowKey() {
        return rowKey;
    }

    public Map getValues() {
        return values;
    }

    public ByteBuffer getCommand() {
        return command;
    }

    public void publish(ByteBuffer command, ByteBuffer rowKey, Map values) {
        this.command = command;
        this.rowKey = rowKey;
        this.values = values;
    }
}
