package org.crustee.raft.storage.table;

import java.util.Comparator;

public interface Timestamped {

    Comparator<Timestamped> TIMESTAMPED_COMPARATOR = (t1, t2) -> Long.compare(t1.getCreationTimestamp(), t2.getCreationTimestamp());

    long getCreationTimestamp();
}
