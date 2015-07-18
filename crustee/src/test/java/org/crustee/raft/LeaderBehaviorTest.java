package org.crustee.raft;

import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import org.crustee.raft.rpc.AppendEntriesResponse;
import org.crustee.raft.rpc.Rpc;
import org.junit.Test;
import org.crustee.raft.rpc.AppendEntries;

public class LeaderBehaviorTest extends AbstractTest {

    //    Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts (§5.2)
    @Test
    public void should_send_heartbeat_on_convert_to_leader() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;

        range(0, 10)
                .mapToObj(i -> new RaftNode(messageBus, DEFAULT_CLOCK))
                .forEach(otherNode -> node.otherNodes.put(otherNode.id, otherNode));

        node.convertToLeader();

        Assertions.assertThat(messages).hasSize(10);
        for (Rpc message : messages) {
            assertThat(message).isInstanceOf(AppendEntries.class);
        }
        messages.stream()
                .map(rpc -> ((AppendEntries) rpc))
                .peek(requestVote -> assertThat(requestVote.getTerm()).isEqualTo(node.currentTerm))
                .peek(requestVote -> assertThat(requestVote.getEntries()).isEmpty());
    }

    // If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
    @Test
    public void should_apply_commands_to_local_log() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;
        node.convertToLeader();

        node.clientCommands.offer(new Command("foo", "bar"));

        node.doLeaderWork();

        assertThat(node.log).hasSize(2);
        LogEntry lastLogEntry = node.lastLogEntry();
        assertThat(lastLogEntry.getKey()).isEqualTo("foo");

        assertThat(lastLogEntry.getValue()).isEqualTo("bar");
        assertThat(lastLogEntry.getTerm()).isEqualTo(42);
    }

    @Test
    public void should_apply_commands_to_state_machine() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.currentTerm = 42;
        node.convertToLeader();

        node.clientCommands.offer(new Command("foo", "bar"));

        node.doLeaderWork();

        assertThat(node.stateMachine.get("foo")).isEqualTo("bar");
    }

    // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    @Test
    public void should_send_entries_for_followers_with_next_index_lower_than_last_index() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.currentTerm = 42;
        node.convertToLeader();

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 0);

        node.doLeaderWork();

        Assertions.assertThat(messages).hasSize(1);
        Assertions.assertThat(messages.get(0)).isInstanceOf(AppendEntries.class);
        AppendEntries appendEntries = (AppendEntries) messages.get(0);
        assertThat(appendEntries.getEntries()).hasSize(1);
        LogEntry entry = appendEntries.getEntries().get(0);
        assertThat(entry.getKey()).isEqualTo("key");
        assertThat(entry.getValue()).isEqualTo("value1");
        assertThat(entry.getTerm()).isEqualTo(23);
    }

    //If successful: update nextIndex and matchIndex for follower (§5.3)
    @Test
    public void should_update_match_and_next_index_if_send_successful() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.currentTerm = 42;
        node.convertToLeader();

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 2);
        node.matchIndex.put(otherNode.id, 2);

        node.rpcs.offer(new AppendEntriesResponse(42, true, otherNode.id, node.id, 10));

        node.doWork();

        assertThat(node.nextIndex.get(otherNode.id)).isEqualTo(10);
    }

    // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
    @Test
    public void should_decrement_match_and_next_index_if_send_unsuccessful() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.log.add(new LogEntry(2, "key", "value1", 23));
        node.log.add(new LogEntry(3, "key", "value1", 23));
        node.currentTerm = 42;
        node.lastApplied = 3;

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.convertToLeader();
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 2);
        node.matchIndex.put(otherNode.id, 2);

        node.rpcs.offer(new AppendEntriesResponse(42, false, otherNode.id, node.id, 10));

        node.doWork();
        node.doWork();

        assertThat(node.nextIndex.get(otherNode.id)).isEqualTo(1);

        Assertions.assertThat(messages).hasSize(1);
        Assertions.assertThat(messages.get(0)).isInstanceOf(AppendEntries.class);
        AppendEntries appendEntries = (AppendEntries) messages.get(0);
        assertThat(appendEntries.getEntries()).hasSize(1);
    }

    // If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
    @Test
    public void should_update_commit_if_quorum() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.log.add(new LogEntry(2, "key", "value2", 23));
        node.log.add(new LogEntry(3, "key", "value3", 24));
        node.log.add(new LogEntry(4, "key", "value4", 41));
        node.log.add(new LogEntry(5, "key", "value5", 42));
        node.currentTerm = 42;
        node.commitIndex = 3;
        node.convertToLeader();

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 4);
        node.matchIndex.put(otherNode.id, 4);

        RaftNode otherNode2 = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode2.id, otherNode);
        node.nextIndex.put(otherNode2.id, 4);
        node.matchIndex.put(otherNode2.id, 4);

        node.rpcs.offer(new AppendEntriesResponse(42, false, otherNode.id, node.id, 10));

        node.checkUpdateCommitIndex();

        assertThat(node.commitIndex).isEqualTo(4);
    }

    @Test
    public void should_update_commit_when_logentry_term_not_equal_to_current_term() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.log.add(new LogEntry(2, "key", "value2", 23));
        node.log.add(new LogEntry(3, "key", "value3", 24));
        node.log.add(new LogEntry(4, "key", "value4", 41));
        node.log.add(new LogEntry(5, "key", "value5", 41));
        node.currentTerm = 42;
        node.commitIndex = 3;
        node.convertToLeader();

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 4);
        node.matchIndex.put(otherNode.id, 4);

        RaftNode otherNode2 = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode2.id, otherNode);
        node.nextIndex.put(otherNode2.id, 4);
        node.matchIndex.put(otherNode2.id, 4);

        node.rpcs.offer(new AppendEntriesResponse(42, false, otherNode.id, node.id, 10));

        node.checkUpdateCommitIndex();

        assertThat(node.commitIndex).isEqualTo(3);
    }

    @Test
    public void should_not_update_commit_without_quorum() {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.log.add(new LogEntry(1, "key", "value1", 23));
        node.log.add(new LogEntry(2, "key", "value2", 23));
        node.log.add(new LogEntry(3, "key", "value3", 24));
        node.log.add(new LogEntry(4, "key", "value4", 41));
        node.log.add(new LogEntry(5, "key", "value5", 42));
        node.currentTerm = 42;
        node.commitIndex = 3;
        node.convertToLeader();

        RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.otherNodes.put(otherNode.id, otherNode);
        node.nextIndex.put(otherNode.id, 4);
        node.matchIndex.put(otherNode.id, 4);

        node.rpcs.offer(new AppendEntriesResponse(42, false, otherNode.id, node.id, 10));

        node.checkUpdateCommitIndex();

        assertThat(node.commitIndex).isEqualTo(3);
    }

}
