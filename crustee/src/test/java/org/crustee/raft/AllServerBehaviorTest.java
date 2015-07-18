package org.crustee.raft;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Collections;
import java.util.EnumSet;
import org.crustee.raft.rpc.AppendEntriesResponse;
import org.junit.Test;
import org.crustee.raft.rpc.AppendEntries;
import org.crustee.raft.rpc.RequestVoteResponse;

public class AllServerBehaviorTest extends AbstractTest {

    static EnumSet<Role> roles = EnumSet.allOf(Role.class);
    static EnumSet<Role> rolesExceptFollower = EnumSet.of(Role.LEADER, Role.CANDIDATE);

    @Test
    public void should_increment_last_applied_index_if_commit_is_greater() throws Exception {
        for (Role role : roles) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.role = role;
            node.lastApplied = 1;
            node.commitIndex = 42;
            node.log.add(new LogEntry(1, "key", "value1", 1));
            node.log.add(new LogEntry(2, "key", "value2", 1));
            node.log.add(new LogEntry(3, "key", "value3", 1));

            node.doWork();

            assertThat(node.lastApplied).isEqualTo(2);
            assertThat(node.stateMachine.get("key")).isEqualTo("value3");
        }
    }

    @Test
    public void should_not_increment_last_applied_index_if_commit_is_equal() throws Exception {
        for (Role role : roles) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.role = role;
            addLeaderStructureIfNecessary(node);
            node.lastApplied = 1;
            node.commitIndex = 1;
            node.log.add(new LogEntry(1, "key", "value1", 1));

            node.doWork();

            assertThat(node.lastApplied).isEqualTo(1);
        }
    }

    @Test
    public void should_set_term_and_revert_to_follower_on_rpc_with_greater_term() throws Exception {
        for (Role role : roles) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.role = role;
            node.currentTerm = 42;
            node.log.add(new LogEntry(1, "key", "value1", 1));

            node.rpcs.offer(new AppendEntries(1024, "leader", 23, 42, Collections.emptyList(), 72, "source", "target"));
            node.doWork();

            assertThat(node.currentTerm).isEqualTo(1024);
            assertThat(node.role).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    public void should_set_term_and_revert_to_follower_on_AppendEntriesResponse_with_greater_term() throws Exception {
        for (Role role : rolesExceptFollower) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.role = role;
            addLeaderStructureIfNecessary(node);
            node.currentTerm = 42;

            node.log.add(new LogEntry(1, "key", "value1", 1));

            RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.otherNodes.put(otherNode.id, otherNode);

            node.rpcs.offer(new AppendEntriesResponse(1024, false, otherNode.id, node.id, -1));
            node.doWork();

            assertThat(node.currentTerm).isEqualTo(1024);
            assertThat(node.role).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    public void should_set_term_and_revert_to_follower_on_RequestVoteResponse_with_greater_term() throws Exception {
        for (Role role : roles) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.role = role;
            addLeaderStructureIfNecessary(node);
            node.currentTerm = 42;

            node.log.add(new LogEntry(1, "key", "value1", 1));

            RaftNode otherNode = new RaftNode(messageBus, DEFAULT_CLOCK);
            node.otherNodes.put(otherNode.id, otherNode);

            node.rpcs.offer(new RequestVoteResponse(1024, false, otherNode.id, node.id));
            node.doWork();

            assertThat(node.currentTerm).isEqualTo(1024);
            assertThat(node.role).isEqualTo(Role.FOLLOWER);
        }
    }

    @Test
    public void should_not_change_term_and_revert_to_follower_on_rpc_with_same_or_lower_term() throws Exception {
        for (Role role : rolesExceptFollower) {
            RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
            addLeaderStructureIfNecessary(node);
            node.currentTerm = 42;
            node.role = role;
            node.log.add(new LogEntry(1, "key", "value1", 1));

            node.rpcs.offer(new AppendEntries(41, "leader", 23, 42, Collections.emptyList(), 72, "source", "target"));
            node.doWork();

            assertThat(node.currentTerm).isEqualTo(42);
            assertThat(node.role).isEqualTo(role);
        }
    }

    private void addLeaderStructureIfNecessary(RaftNode node) {
        if(node.role == Role.LEADER) {
            // cheat by having leader-only structure initialized to avoid failing tests with exceptions when doing leader-related work
            node.convertToLeader();
        }
    }

}
