package org.crustee.raft.storage;

import java.util.Map;
import java.util.function.BiConsumer;
import org.crustee.raft.storage.row.Row;

public interface Memtable {

    <K> void applyInOrder(BiConsumer<K, Row> action);

    int getCount();

    <K> Row get(K key);

    /**
     * Modifications are isolated at the row level.
     */
    <K1, K2, V> void insert(K1 rowKey, Map<K2, V> values);

    void freeze();
}
