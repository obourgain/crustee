package org.crustee.raft;

import java.util.HashMap;
import java.util.Map;
import org.crustee.raft.rpc.Rpc;

public class MessageBus {

    Map<String, RaftNode> nodes = new HashMap<>();

    public void send(String targetId, Rpc rpc) {
        nodes.get(targetId).rpcs.offer(rpc);
    }

    public void register(RaftNode raftNode) {
        nodes.put(raftNode.id, raftNode);
    }

}
