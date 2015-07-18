package org.crustee.raft;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import org.junit.Test;
import org.crustee.raft.rpc.AppendEntries;
import org.crustee.raft.rpc.AppendEntriesResponse;


public class AppendEntriesTest extends AbstractTest {

    @Test
    public void should_reply_false_if_message_term_is_less_than_node_term() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 1));

        node.rpcs.offer(new AppendEntries(41, "leader", 23, 42, emptyList(), 72, "source", "target"));
        node.doWork();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(AppendEntriesResponse.class);
        AppendEntriesResponse response = (AppendEntriesResponse) messages.get(0);

        assertThat(response.isSuccess()).isFalse();
        assertThat(response.getTerm()).isEqualTo(node.currentTerm);
    }

    @Test
    public void should_reply_false_if_log_doesnt_contain_entry_at_prevLogIndex_with_term_equal_prevLogTerm() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.log.add(new LogEntry(2, "key", "value1", 42));

        node.rpcs.offer(new AppendEntries(1024, "leader", 1, 41, emptyList(), 72, "source", "target"));
        node.doWork();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(AppendEntriesResponse.class);
        AppendEntriesResponse response = (AppendEntriesResponse) messages.get(0);

        assertThat(response.isSuccess()).isFalse();
    }

    @Test
    public void should_delete_conflicting_entry_and_following_on_conflict() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 41;
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.log.add(new LogEntry(2, "key", "value2", 41));
        node.log.add(new LogEntry(3, "key", "value3", 40)); // should be deleted
        node.log.add(new LogEntry(4, "key", "value4", 40)); // should be deleted

        ArrayList<LogEntry> newEntries = new ArrayList<>();
        newEntries.add(new LogEntry(3, "key", "value3", 41));

        node.rpcs.offer(new AppendEntries(41, "leader", 2, 40, newEntries, 72, "source", "target"));
        node.doWork();

        assertThat(node.log).hasSize(3);
    }

    @Test
    public void should_append_entries_not_already_in_the_log() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 41;
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.log.add(new LogEntry(2, "key", "value2", 41));
        node.log.add(new LogEntry(3, "key", "value3", 40));
        node.log.add(new LogEntry(4, "key", "value4", 40));

        ArrayList<LogEntry> newEntries = new ArrayList<>();
        newEntries.add(new LogEntry(3, "key", "value3", 40));
        LogEntry expectedNewEntry = new LogEntry(4, "key", "value3", 41);
        newEntries.add(expectedNewEntry);

        node.rpcs.offer(new AppendEntries(41, "leader", 2, 40, newEntries, 72, "source", "target"));
        node.doWork();

        assertThat(node.log).hasSize(5);
        assertThat(node.log.get(4)).isEqualTo(expectedNewEntry);
    }

    @Test
    public void should_update_commit_to_leader_commit() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 41;
        node.commitIndex = 2;
        node.lastApplied = 2;
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.log.add(new LogEntry(2, "key", "value2", 41));
        node.log.add(new LogEntry(3, "key", "value3", 40));
        node.log.add(new LogEntry(4, "key", "value4", 40));

        ArrayList<LogEntry> newEntries = new ArrayList<>();
        newEntries.add(new LogEntry(3, "key", "value3", 40));
        LogEntry expectedNewEntry = new LogEntry(4, "key", "value3", 41);
        newEntries.add(expectedNewEntry);

        node.rpcs.offer(new AppendEntries(41, "leader", 2, 40, newEntries, 3, "source", "target"));
        node.doWork();

        assertThat(node.commitIndex).isEqualTo(3);
    }

    @Test
    public void should_update_commit_to_last_entry_index() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 41;
        node.commitIndex = 2;
        node.lastApplied = 2;
        node.log.add(new LogEntry(1, "key", "value1", 1));
        node.log.add(new LogEntry(2, "key", "value2", 41));
        node.log.add(new LogEntry(3, "key", "value3", 40));
        node.log.add(new LogEntry(4, "key", "value4", 40));

        ArrayList<LogEntry> newEntries = new ArrayList<>();
        newEntries.add(new LogEntry(3, "key", "value3", 40));
        LogEntry expectedNewEntry = new LogEntry(4, "key", "value3", 41);
        newEntries.add(expectedNewEntry);

        node.rpcs.offer(new AppendEntries(41, "leader", 2, 40, newEntries, 5, "source", "target"));
        node.doWork();

        assertThat(node.commitIndex).isEqualTo(4);
    }

}
