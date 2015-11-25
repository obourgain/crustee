package org.crustee.raft.storage.table;

import java.util.Comparator;

public interface Timestamped {

    Comparator<Timestamped> TIMESTAMPED_COMPARATOR = Comparator.comparing (Timestamped::getCreationTimestamp);

    long getCreationTimestamp();
}
