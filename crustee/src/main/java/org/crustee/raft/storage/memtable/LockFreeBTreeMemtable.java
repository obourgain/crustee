package org.crustee.raft.storage.memtable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.crustee.raft.storage.commitlog.Segment;
import org.crustee.raft.storage.row.MapRow;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.ByteBufferUtils;

public class LockFreeBTreeMemtable implements WritableMemtable {

    private final LockFreeBTree<ByteBuffer, Row> bTree = new LockFreeBTree<>(ByteBufferUtils.lengthFirstComparator(), 16, ByteBuffer.class, Row.class);

    private volatile long estimatedSizeInBytes = 0;

    private final long creationTimestamp;

    @VisibleForTesting
    protected final List<Segment> segments = new ArrayList<>();

    public LockFreeBTreeMemtable(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

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
        assert rowKey.limit() <= Short.MAX_VALUE : "row key must not be larger than " + Short.MAX_VALUE + " bytes";

        LockFreeBTree.UpdateAction<Row> updateAction = new UpdateRow(rowKey, values);
        bTree.insert(rowKey, updateAction);
    }

    @Override
    public void addSegment(Segment segment) {
        segment.acquire();
        this.segments.add(segment);
    }

    @Override
    public long getEstimatedSizeInBytes() {
        return estimatedSizeInBytes;
    }

    @Override
    public void close() {
        segments.forEach(Segment::release);
    }

    @Override
    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    @Override
    public ReadOnlyMemtable freeze() {
        bTree.freeze();
        return this;
    }

    private boolean areAtPosition0(Map<ByteBuffer, ByteBuffer> values) {
        // this method returns a boolean because it should be used in an assert statement to allow the whole method to
        // be removed by dead code elimination
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
