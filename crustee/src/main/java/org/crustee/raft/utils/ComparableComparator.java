package org.crustee.raft.utils;

import java.util.Comparator;

public class ComparableComparator<K> implements Comparator<K> {

    private static final ComparableComparator instance = new ComparableComparator();

    private ComparableComparator() {
    }

    @Override
    public int compare(K o1, K o2) {
        assert o1 != null && o1 instanceof Comparable;
        assert o2 != null && o2.getClass() == o1.getClass() : "expecting same class, got " + o1.getClass() + " / " + (o2 == null ? null : o2.getClass());
        return ((Comparable<K>) o1).compareTo(o2);
    }

    public static <K> Comparator<K> get() {
        return (Comparator<K>) instance;
    }

}
