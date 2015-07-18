package org.crustee.raft.rpc;


import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RequestVoteResponse implements Rpc {
    long term;
    boolean voteGranted;
    String sourceNodeId;
    String targetNodeId;
}
