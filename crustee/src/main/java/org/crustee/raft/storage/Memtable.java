package org.crustee.raft.storage;

import java.util.function.BiConsumer;

public interface Memtable {

    <K, V> void applyInOrder(BiConsumer<K, V> action);

    int getCount();

    <V> V get(V key);

    <V> void insert(V key, V value);

    void freeze();
}
