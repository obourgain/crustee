package org.crustee.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RequestVote implements Rpc {
    long term;
    String candidateId;
    long lastLogIndex;
    long lastLogTerm;
    String sourceNodeId;
    String targetNodeId;
}
