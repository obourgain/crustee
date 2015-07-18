package org.crustee.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class InstallSnapshotResponse implements Rpc {

    long term;

    String sourceNodeId;
    String targetNodeId;

}
