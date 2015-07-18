package org.crustee.raft;

import static org.crustee.raft.Role.CANDIDATE;
import static org.crustee.raft.Role.FOLLOWER;
import static org.crustee.raft.Role.LEADER;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.crustee.raft.rpc.AppendEntries;
import org.crustee.raft.rpc.RequestVote;
import org.crustee.raft.rpc.RequestVoteResponse;
import org.crustee.raft.rpc.Rpc;
import org.crustee.raft.util.SettableClock;

public class CandidateBehaviorTest extends AbstractTest {

    @Test
    public void should_increment_term_on_convert_to_candidate() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        node.convertToCandidate();

        assertThat(node.currentTerm).isEqualTo(43);
    }

    @Test
    public void should_vote_for_self_on_convert_to_candidate() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);

        node.convertToCandidate();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(RequestVote.class);
        RequestVote requestVote = (RequestVote) messages.get(0);
        assertThat(requestVote.getCandidateId()).isEqualTo(node.id);
    }

    @Test
    public void should_reset_election_timer_on_convert_to_candidate() {
        SettableClock clock = new SettableClock(1_000);
        RaftNode node = new RaftNode(messageBus, clock);

        node.log.add(new LogEntry(1, "key", "value1", 1));

        clock.setTime(1_000_000);
        node.convertToCandidate();

        assertThat(node.lastHeartbeat).isEqualTo(1_000_000);
        assertThat(node.electTimeout).isBetween(150l, 300l);
    }

    @Test
    public void should_send_request_vote_to_all_other_nodes_on_convert_to_candidate() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        range(0, 10)
                .mapToObj(i -> new RaftNode(messageBus, DEFAULT_CLOCK))
                .forEach(otherNode -> node.otherNodes.put(otherNode.id, otherNode));

        node.convertToCandidate();

        assertThat(messages).hasSize(node.otherNodes.size());
        for (Rpc message : messages) {
            assertThat(message).isInstanceOf(RequestVote.class);
        }

        long count = messages.stream()
                .map(rpc -> ((RequestVote) rpc))
                .peek(requestVote -> {
                    assertThat(requestVote.getTerm()).isEqualTo(43);
                    assertThat(requestVote.getCandidateId()).isEqualTo(node.id);
                    assertThat(requestVote.getLastLogIndex()).isEqualTo(node.lastLogEntry().getIndex());
                    assertThat(requestVote.getLastLogTerm()).isEqualTo(node.lastLogEntry().getTerm());
                    assertThat(requestVote.getSourceNodeId()).isEqualTo(node.id);
                })
                .map(RequestVote::getTargetNodeId)
                .distinct().count();
        assertThat(count).isEqualTo(10);
    }

    @Test
    public void should_convert_to_leader_if_received_majority_of_votes() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        range(0, 10)
                .mapToObj(i -> new RaftNode(messageBus, DEFAULT_CLOCK))
                .forEach(otherNode -> node.otherNodes.put(otherNode.id, otherNode));

        node.convertToCandidate();

        for (RaftNode otherNode : node.otherNodes.values()) {
            node.rpcs.offer(new RequestVoteResponse(node.currentTerm, true, otherNode.id, node.id));
            node.doWork();
        }

        assertThat(node.role).isEqualTo(LEADER);
    }

    @Test
    public void should_convert_to_leader_if_have_not_received_majority_of_votes() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        List<RaftNode> otherNodes = range(0, 10)
                .mapToObj(i -> new RaftNode(messageBus, DEFAULT_CLOCK))
                .peek(otherNode -> node.otherNodes.put(otherNode.id, otherNode))
                .collect(Collectors.toList());
        node.convertToCandidate();

        // only two votes on 10
        node.rpcs.offer(new RequestVoteResponse(node.currentTerm, true, otherNodes.get(0).id, node.id));
        node.rpcs.offer(new RequestVoteResponse(node.currentTerm, true, otherNodes.get(1).id, node.id));

        node.doWork();

        assertThat(node.role).isEqualTo(CANDIDATE);
    }

    @Test
    public void should_convert_to_follower_if_received_append_entries_from_new_leader() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;
        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.convertToCandidate();

        node.rpcs.offer(new AppendEntries(43, otherNode.id, 2, 42, Collections.emptyList(), otherNode.commitIndex, otherNode.id, node.id));
        node.doWork();

        assertThat(node.role).isEqualTo(FOLLOWER);
    }

    @Test
    public void should_restart_election_on_election_timeout() {
        SettableClock clock = new SettableClock(1_000);
        RaftNode node = new RaftNode(messageBus, clock);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;
        clock.setTime(2_000);
        node.doWork();

        assertThat(node.role).isEqualTo(CANDIDATE);
        assertThat(node.currentTerm).isEqualTo(43);

        clock.setTime(3_000);
        node.doWork();

        assertThat(node.role).isEqualTo(CANDIDATE);
        assertThat(node.currentTerm).isEqualTo(44);
    }

}
