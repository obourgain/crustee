package org.crustee.raft.storage.sstable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class Buffers {

    private ByteBuffer tableEntryMetadata, indexEntryMetadata, tableKeyBuffer, indexKeyBuffer, valueBuffer;

    public Buffers(ByteBuffer tableEntryMetadata, ByteBuffer indexEntryMetadata, ByteBuffer tableKeyBuffer, ByteBuffer indexKeyBuffer, ByteBuffer valueBuffer) {
        this.tableEntryMetadata = tableEntryMetadata;
        this.indexEntryMetadata = indexEntryMetadata;
        this.tableKeyBuffer = tableKeyBuffer;
        this.indexKeyBuffer = indexKeyBuffer;
        this.valueBuffer = valueBuffer;
    }

    public ByteBuffer tableEntryMetadata() {
        return (ByteBuffer) tableEntryMetadata.clear();
    }

    public ByteBuffer indexEntryMetadata() {
        return (ByteBuffer) indexEntryMetadata.clear();
    }

    public ByteBuffer tableKeyBuffer(int requiredSize) {
        return bufferOfSize(tableKeyBuffer, requiredSize, this::setTableKeyBuffer);
    }

    public ByteBuffer indexKeyBuffer(int requiredSize) {
        return bufferOfSize(indexKeyBuffer, requiredSize, this::setIndexKeyBuffer);
    }

    public ByteBuffer valueBuffer(int requiredSize) {
        return bufferOfSize(valueBuffer, requiredSize, this::setValueBuffer);
    }

    private ByteBuffer bufferOfSize(ByteBuffer current, int requiredSize, Consumer<ByteBuffer> setter) {
        if (requiredSize == current.capacity()) {
            current.clear();
            return current;
        }
        if (requiredSize < current.capacity()) {
            current.position(current.capacity() - requiredSize);
            return current.slice();
        }
        ByteBuffer buffer = ByteBuffer.allocate(requiredSize);
        setter.accept(buffer);
        return buffer;
    }

    public void setTableKeyBuffer(ByteBuffer tableKeyBuffer) {
        this.tableKeyBuffer = tableKeyBuffer;
    }

    public void setIndexKeyBuffer(ByteBuffer indexKeyBuffer) {
        this.indexKeyBuffer = indexKeyBuffer;
    }

    public void setValueBuffer(ByteBuffer valueBuffer) {
        this.valueBuffer = valueBuffer;
    }
}
