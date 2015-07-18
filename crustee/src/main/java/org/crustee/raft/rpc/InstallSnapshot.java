package org.crustee.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class InstallSnapshot implements Rpc {
    long term;
    String leaderId;

    long lastIncludedIndex;
    long lastIncludedTerm;
    long offset;
    byte[] data;
    boolean done;

    String sourceNodeId;
    String targetNodeId;

}
