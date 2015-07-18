package org.crustee.raft.storage.write;

import java.nio.ByteBuffer;

public class WriteEvent {

    private ByteBuffer key;
    private ByteBuffer value;

    public ByteBuffer getKey() {
        return key;
    }

    public void setKey(ByteBuffer key) {
        this.key = key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }

}
