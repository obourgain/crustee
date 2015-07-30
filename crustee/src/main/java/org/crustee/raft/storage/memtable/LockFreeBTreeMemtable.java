package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiConsumer;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.crustee.raft.storage.row.MapRow;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.ByteBufferUtils;

public class LockFreeBTreeMemtable implements Memtable {

    private final LockFreeBTree<ByteBuffer, Row> bTree = new LockFreeBTree<>(ByteBufferUtils.lengthFirstComparator(), 16, ByteBuffer.class, Row.class);

    private volatile long estimatedSizeInBytes = 0;

    @Override
    public void applyInOrder(BiConsumer<ByteBuffer, Row> action) {
        bTree.applyInOrder(action);
    }

    @Override
    public int getCount() {
        return bTree.getCount();
    }

    @Override
    public Row get(ByteBuffer key) {
        return bTree.get(key);
    }

    @Override
    public void insert(ByteBuffer rowKey, Map<ByteBuffer, ByteBuffer> values) {
        assert rowKey.position() == 0;
        assert areAtPosition0(values);

        LockFreeBTree.UpdateAction<Row> updateAction = new UpdateRow(rowKey, values);
        bTree.insert(rowKey, updateAction);
    }

    @Override
    public long getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }

    @Override
    public void freeze() {
        bTree.freeze();
    }


    private boolean areAtPosition0(Map<ByteBuffer, ByteBuffer> values) {
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
            assert entry.getKey().position() == 0;
            assert entry.getValue().position() == 0;
        }
        return true;
    }

    private class UpdateRow implements LockFreeBTree.UpdateAction<Row> {

        private final ByteBuffer rowKey;
        private final Map<ByteBuffer, ByteBuffer> values;

        private UpdateRow(ByteBuffer rowKey, Map<ByteBuffer, ByteBuffer> values) {
            this.rowKey = rowKey;
            this.values = values;
        }

        @Override
        public Row merge(Row current) {
            long sizeBefore = current.getEstimatedSizeInBytes();
            current.addAll(values); // TODO clone inside the Row or here ?
            estimatedSizeInBytes += current.getEstimatedSizeInBytes() - sizeBefore;
            return current;
        }

        @Override
        public Row insert() {
            MapRow row = new MapRow(values);
            estimatedSizeInBytes += row.getEstimatedSizeInBytes() + rowKey.limit();
            return row;
        }
    }

}
