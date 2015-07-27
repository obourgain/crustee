package org.crustee.raft.utils;

import java.util.Comparator;

public class ComparableComparator implements Comparator<Object> {

    private static final ComparableComparator instance = new ComparableComparator();

    private ComparableComparator() {
    }

    @Override
    public int compare(Object o1, Object o2) {
        assert o1 != null && o1 instanceof Comparable;
        assert o2 != null && o2.getClass() == o1.getClass() : "expecting same class, got " + o1.getClass() + " / " + (o2 == null ? null : o2.getClass());
        return ((Comparable) o1).compareTo(o2);
    }

    public static Comparator<Object> get() {
        return instance;
    }
}
