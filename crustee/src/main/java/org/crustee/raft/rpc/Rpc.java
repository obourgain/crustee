package org.crustee.raft.rpc;

public interface Rpc {

    String getSourceNodeId();
    String getTargetNodeId();
    long getTerm();

}
