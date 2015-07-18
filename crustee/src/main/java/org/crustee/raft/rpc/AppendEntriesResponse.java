package org.crustee.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AppendEntriesResponse implements Rpc {
    long term;

    boolean success;
    String sourceNodeId;
    String targetNodeId;

    long newLogIndex;
}
