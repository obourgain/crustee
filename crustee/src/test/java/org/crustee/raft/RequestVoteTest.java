package org.crustee.raft;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import org.crustee.raft.rpc.RequestVote;
import org.crustee.raft.rpc.RequestVoteResponse;


public class RequestVoteTest extends AbstractTest {

    @Test
    public void should_reply_false_if_term_is_less_than_current_term() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 1));

        node.rpcs.offer(new RequestVote(41, "candidate", 23, 42, "candidate", node.id));
        node.doWork();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(RequestVoteResponse.class);
        RequestVoteResponse response = (RequestVoteResponse) messages.get(0);

        assertThat(response.isVoteGranted()).isFalse();
        assertThat(response.getTerm()).isEqualTo(node.currentTerm);
    }

    @Test
    public void should_vote_for_requester_when_not_already_voted_and_candidate_log_length_is_up_to_date() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 42));

        // candidate have a more up to date log (longer) with same term as node
        node.rpcs.offer(new RequestVote(43, "candidate", 23, 42, "candidate", node.id));
        node.doWork();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(RequestVoteResponse.class);
        RequestVoteResponse response = (RequestVoteResponse) messages.get(0);

        assertThat(response.isVoteGranted()).isTrue();
        assertThat(response.getTerm()).isEqualTo(node.currentTerm);
    }

    @Test
    public void should_vote_for_requester_when_not_already_voted_and_candidate_log_term_is_up_to_date() throws Exception {
        RaftNode node = new RaftNode(messageBus, DEFAULT_CLOCK);
        node.currentTerm = 42;
        node.log.add(new LogEntry(1, "key", "value1", 42));

        // candidate have a more up to date log according to term
        node.rpcs.offer(new RequestVote(43, "candidate", 1, 43, "candidate", node.id));
        node.doWork();

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isInstanceOf(RequestVoteResponse.class);
        RequestVoteResponse response = (RequestVoteResponse) messages.get(0);

        assertThat(response.isVoteGranted()).isTrue();
        assertThat(response.getTerm()).isEqualTo(node.currentTerm);
    }

}
