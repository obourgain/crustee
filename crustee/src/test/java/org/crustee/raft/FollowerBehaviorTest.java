package org.crustee.raft;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import org.crustee.raft.rpc.InstallSnapshot;
import org.crustee.raft.rpc.InstallSnapshotResponse;
import org.crustee.raft.rpc.RequestVote;
import org.crustee.raft.util.SettableClock;
import org.junit.Test;

public class FollowerBehaviorTest extends AbstractTest {

    @Test
    public void should_convert_to_candidate_on_timeout() throws Exception {
//        If election timeout elapses without receiving AppendEntries
//        RPC from current leader or granting vote to candidate:
        SettableClock clock = new SettableClock(1000);
        RaftNode node = new RaftNode(messageBus, clock);
        node.currentTerm = 42;
        node.role = Role.FOLLOWER;
        node.lastHeartbeat = clock.millis();
        clock.setTime(2000);
        node.log.add(new LogEntry(1, "key", "value1", 1));

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);

        node.doWork();

        assertThat(node.role).isEqualTo(Role.CANDIDATE);
        assertThat(node.currentTerm).isEqualTo(43);
        assertThat(node.votedFor).isEqualTo(node.id);

        Assertions.assertThat(messages).hasSize(1);
        Assertions.assertThat(messages.get(0)).isInstanceOf(RequestVote.class);
        RequestVote requestVote = (RequestVote) messages.get(0);
        assertThat(requestVote.getTerm()).isEqualTo(43);
        assertThat(requestVote.getCandidateId()).isEqualTo(node.id);
        assertThat(requestVote.getLastLogIndex()).isEqualTo(node.lastLogEntry().getIndex());
        assertThat(requestVote.getLastLogTerm()).isEqualTo(node.lastLogEntry().getTerm());
        assertThat(requestVote.getSourceNodeId()).isEqualTo(node.id);
        assertThat(requestVote.getTargetNodeId()).isEqualTo(otherNode.id);

    }

    @Test
    public void should_reject_install_snapshot_with_lower_term() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 1));

        node.rpcs.offer(new InstallSnapshot(41, "source", 23, 27, 12, new byte[42], false, "source", node.id));
        node.doWork();

        Assertions.assertThat(messages).hasSize(1);
        Assertions.assertThat(messages.get(0)).isInstanceOf(InstallSnapshotResponse.class);
        InstallSnapshotResponse requestVote = (InstallSnapshotResponse) messages.get(0);
        assertThat(requestVote.getTerm()).isEqualTo(42);
        assertThat(requestVote.getSourceNodeId()).isEqualTo(node.id);
        assertThat(requestVote.getTargetNodeId()).isEqualTo("source");
    }

}
