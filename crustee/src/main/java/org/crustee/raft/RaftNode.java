package org.crustee.raft;

import static java.util.Collections.singletonList;
import static org.slf4j.LoggerFactory.getLogger;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.crustee.raft.rpc.AppendEntries;
import org.crustee.raft.rpc.AppendEntriesResponse;
import org.crustee.raft.rpc.InstallSnapshot;
import org.crustee.raft.rpc.InstallSnapshotResponse;
import org.crustee.raft.rpc.RequestVote;
import org.crustee.raft.rpc.RequestVoteResponse;
import org.crustee.raft.rpc.Rpc;
import org.slf4j.Logger;

public class RaftNode {

    final MessageBus messageBus;
    final Clock clock;

    protected Queue<Rpc> rpcs;
    protected long lastHeartbeat;
    protected long electTimeout = randomTimeOut();
    int votedForThisNodeCount = 0;

    protected Map<String, RaftNode> otherNodes = new HashMap<>();

    // persistent
    final String id = UUID.randomUUID().toString();
    protected long currentTerm;
    protected String votedFor;
    // TODO use a collection with long indices
    protected List<LogEntry> log = new ArrayList<>();
    // volatile
    protected int commitIndex = 0;
    protected int lastApplied = 0;
    // leader state
    protected Map<String, Integer> nextIndex;
    protected Map<String, Integer> matchIndex;

    protected Queue<Command> clientCommands = new LinkedList<>();
    protected Map<String, String> stateMachine = new HashMap<>();

    Role role = Role.FOLLOWER;

    private final Logger logger = getLogger("raft-node-" + id);

    protected void addLogEntry(LogEntry logEntry) {
        log.add(logEntry);
    }

    public RaftNode(MessageBus messageBus, Clock clock) {
        this.messageBus = messageBus;
        this.clock = clock;
        this.rpcs = new LinkedBlockingQueue<>();
        lastHeartbeat = clock.millis();

        messageBus.register(this);
    }

    protected void doWork() {
        RulesLoop rulesLoop = applyLog();
        if (rulesLoop == RulesLoop.NEXT_LOOP) return;

        Rpc rpc = rpcs.poll();
        if (rpc != null) {
            rulesLoop = handleRpc(rpc);
            if (rulesLoop == RulesLoop.NEXT_LOOP) return;
        }

        long deadline = lastHeartbeat + electTimeout;
        long current = clock.millis();
        if (current > deadline) {
            if (role == Role.CANDIDATE) {
                logger.info("election timeout elapsed, deadline : {}, current: {}, restarting election", deadline, current);
            } else {
                logger.info("heartbeat timeout elapsed, deadline : {}, current: {}, converting to candidate", deadline, current);
            }
            convertToCandidate();
            return;
        }


        if (role == Role.LEADER) {
            doLeaderWork();
        } else {
            Command command = clientCommands.peek();
            if (command != null) {
                // notify client to talk to leader
            }
        }
    }

    protected void doLeaderWork() {
        Command command = clientCommands.poll();
        if (command != null) {
            log.add(new LogEntry(log.size(), command.getKey(), command.getValue(), currentTerm));
            stateMachine.put(command.getKey(), command.getValue());
            // persist
            // respond to client
        }

        for (Map.Entry<String, Integer> entry : nextIndex.entrySet()) {
            // TODO batch entries
            int next = entry.getValue();
            if (lastApplied >= next) {
                LogEntry entryToSend = log.get(next);
                messageBus.send(entry.getKey(),
                        new AppendEntries(currentTerm, id, log.size() - 1, lastLogEntry().term, singletonList(entryToSend), commitIndex, id, entry.getKey()));
            }
        }

        checkUpdateCommitIndex();
    }

    protected void checkUpdateCommitIndex() {
        TreeMap<Integer, Integer> countPerIndex = new TreeMap<>();
        for (Map.Entry<String, Integer> entry : matchIndex.entrySet()) {
            countPerIndex.compute(entry.getValue(), (k, v) -> v != null ? v + 1 : 1);
        }
        Iterator<Integer> descendingIterator = countPerIndex.navigableKeySet().descendingIterator();
        int quorum = quorum();
        while (descendingIterator.hasNext()) {
            Integer index = descendingIterator.next();
            if (index > commitIndex) {
                Integer count = countPerIndex.get(index);
                if (count >= quorum) {
                    if (log.get(index).term == currentTerm) {
                        commitIndex = index;
                    }
                    break;
                }
            }
        }
    }

    private RulesLoop applyLog() {
        if (commitIndex > lastApplied) {
            lastApplied++;
            LogEntry logEntry = log.get(lastApplied);
            stateMachine.put(logEntry.key, logEntry.value);
            return RulesLoop.NEXT_LOOP;
        }
        return RulesLoop.CONTINUE;
    }

    private RulesLoop handleRpc(Rpc rpc) {
        long rpcTerm = rpc.getTerm();
        if (rpcTerm > currentTerm) {
            this.currentTerm = rpcTerm;
            role = Role.FOLLOWER;
        }

        if (rpc instanceof AppendEntries) {
            AppendEntries appendEntries = (AppendEntries) rpc;
            if (handleAppendEntries(appendEntries)) return RulesLoop.NEXT_LOOP;

        } else if (rpc instanceof RequestVote) {
            if (rpcTerm < currentTerm) {
                // reject
                logger.debug("dropping RequestVote message with term {} lower than current {}", rpcTerm, currentTerm);
                messageBus.send(rpc.getSourceNodeId(), new RequestVoteResponse(currentTerm, false, id, rpc.getSourceNodeId()));
                return RulesLoop.NEXT_LOOP;
            }

            RequestVote requestVote = (RequestVote) rpc;
            return handleRequestVote(rpc, requestVote);

        } else if (rpc instanceof AppendEntriesResponse) {
            if (!(role == Role.LEADER)) {
                logger.warn("received an AppendEntriesResponse but this node is not LEADER");
                return RulesLoop.CONTINUE;
            }
//            • If successful: update nextIndex and matchIndex for
//            follower (§5.3)
//            • If AppendEntries fails because of log inconsistency:
//            decrement nextIndex and retry (§5.3)
            AppendEntriesResponse response = (AppendEntriesResponse) rpc;
            if (response.isSuccess()) {
                Integer currentMatchIndex = matchIndex.get(response.getSourceNodeId());
                long newLogIndex = response.getNewLogIndex();
                assert newLogIndex >= 0; // we can have a negative value only if the AppendEntries is not a success
                logger.debug("setting matchIndex and nextIndex of {} from {} to {}", "'otherID'", currentMatchIndex, newLogIndex);
                matchIndex.put(response.getSourceNodeId(), (int) newLogIndex);
                nextIndex.put(response.getSourceNodeId(), (int) newLogIndex);
            } else {
                Integer currentNextIndex = nextIndex.get(response.getSourceNodeId());
                int newNextIndex = currentNextIndex - 1;
                logger.debug("decrementing nextIndex of {} from {} to {}", "'otherID'", currentNextIndex, newNextIndex);
                nextIndex.put(response.getSourceNodeId(), newNextIndex);
                // retry will be done in doLeaderWork()
            }
            return RulesLoop.NEXT_LOOP;

        } else if (rpc instanceof RequestVoteResponse) {
            RequestVoteResponse requestVoteResponse = (RequestVoteResponse) rpc;
            if (requestVoteResponse.isVoteGranted()) {
                // TODO handle duplicated messages from same peer
                votedForThisNodeCount++;
            }
            if (votedForThisNodeCount > quorum()) {
                convertToLeader();
            }
            return RulesLoop.NEXT_LOOP;
        } else if(rpc instanceof InstallSnapshot) {
            if (rpcTerm < currentTerm) {
                // reject
                logger.debug("dropping InstallSnapshot message with term {} lower than current {}", rpcTerm, currentTerm);
                messageBus.send(rpc.getSourceNodeId(), new InstallSnapshotResponse(currentTerm, id, rpc.getSourceNodeId()));
                return RulesLoop.NEXT_LOOP;
            }
            InstallSnapshot installSnapshot = (InstallSnapshot) rpc;
            if(installSnapshot.getOffset() == 0) {
                // TODO create file
            }

        } else if(rpc instanceof InstallSnapshotResponse) {

        }

        return RulesLoop.CONTINUE;
    }

    private RulesLoop handleRequestVote(Rpc rpc, RequestVote requestVote) {
        if (requestVote.getTerm() < currentTerm) {
            respondToRpc(new RequestVoteResponse(currentTerm, false, id, rpc.getSourceNodeId()));
        } else if ((votedFor == null || votedFor.equals(requestVote.getCandidateId()))
                && requestVote.getLastLogIndex() >= log.size() - 1) {
            respondToRpc(new RequestVoteResponse(currentTerm, true, id, rpc.getSourceNodeId()));
        } else {
            // TODO else ?
            respondToRpc(new RequestVoteResponse(currentTerm, false, id, rpc.getSourceNodeId()));
        }
        return RulesLoop.NEXT_LOOP;
    }

    private boolean handleAppendEntries(AppendEntries appendEntries) {
        long rpcTerm = appendEntries.getTerm();
        if (rpcTerm < currentTerm) {
            // reject
            logger.debug("dropping AppendEntries message with term {} lower than {}", rpcTerm, currentTerm);
            messageBus.send(appendEntries.getSourceNodeId(), new AppendEntriesResponse(currentTerm, false, id, appendEntries.getSourceNodeId(), -1));
            return true;
        }

        if (role == Role.CANDIDATE && rpcTerm >= currentTerm) {
            convertToFollower();
        }
        long prevLogIndex = appendEntries.getPrevLogIndex();
        if (log.size() < prevLogIndex || log.get((int) prevLogIndex).term != appendEntries.getPrevLogTerm()) {
            messageBus.send(appendEntries.getSourceNodeId(), new AppendEntriesResponse(currentTerm, false, id, appendEntries.getSourceNodeId(), -1));
            return true;
        }

        for (LogEntry logEntry : appendEntries.getEntries()) {
            long logEntryIndex = logEntry.getIndex();
            if (log.size() > logEntryIndex) {
                long existingEntryTerm = log.get((int) logEntryIndex).term;
                if (existingEntryTerm != logEntry.getTerm()) {
                    // If an existing entry conflicts with a new one (same index
                    // but different terms), delete the existing entry and all that
                    // follow it (§5.3)
                    logger.trace("logEntryIndex={}, existingEntryTerm={}, rpc.term={}, removing tail from {} to {}", logEntryIndex, existingEntryTerm, rpcTerm, log.size(), logEntryIndex);
                    for (int i = log.size() - 1; i >= prevLogIndex; i--) {
                        log.remove(i); // reverse order because remove from ArrayList, this will change latter
                    }
                    log.add(logEntry);
                }
            } else {
                log.add(logEntry);
            }
        }

        LogEntry lastEntry = appendEntries.lastEntry();
        int lastEntryIndex = lastEntry != null ? (int) appendEntries.lastEntry().getIndex() : Integer.MAX_VALUE;
        if (appendEntries.getLeaderCommit() > commitIndex) {
            commitIndex = (int) Math.min(appendEntries.getLeaderCommit(), lastEntryIndex);
        }
        return false;
    }

    private void convertToFollower() {
        role = Role.FOLLOWER;
        deleteLeaderState();
    }

    private void respondToRpc(Rpc rpc) {
        messageBus.send(rpc.getTargetNodeId(), rpc);
    }

    private int quorum() {
        return ((otherNodes.size() + 1) / 2) + 1;
    }

    protected void convertToLeader() {
        role = Role.LEADER;
        matchIndex = new HashMap<>();
        nextIndex = new HashMap<>();
        int lastLogIndex = lastApplied + 1;
        for (String otherNodeId : otherNodes.keySet()) {
            nextIndex.put(otherNodeId, lastLogIndex);
        }
        broadcast(targetId -> new AppendEntries(currentTerm, id, log.size() - 1, lastLogEntry().term, Collections.emptyList(), commitIndex, id, targetId));
    }

    protected void convertToCandidate() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        LogEntry lastLogEntry = lastLogEntry();
        long lastEntryIndex = lastLogEntry.getIndex();
        long lastEntryTerm = lastLogEntry.getTerm();
        lastHeartbeat = clock.millis();
        electTimeout = randomTimeOut();
        deleteLeaderState();
        broadcast(targetId -> new RequestVote(currentTerm, id, lastEntryIndex, lastEntryTerm, id, targetId));
    }

    private void deleteLeaderState() {
        matchIndex = null;
        nextIndex = null;
    }

    protected long randomTimeOut() {
        return (long) (150 + (Math.random() * 150));
    }

    private void broadcast(Function<String, Rpc> rpc) {
        for (Map.Entry<String, RaftNode> entry : otherNodes.entrySet()) {
            messageBus.send(entry.getKey(), rpc.apply(entry.getKey()));
        }
    }

    protected LogEntry lastLogEntry() {
        int lastIndex = log.size() - 1;
        return log.get(lastIndex);
    }

    private enum RulesLoop {
        CONTINUE,
        NEXT_LOOP,
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftNode raftNode = (RaftNode) o;

        return id.equals(raftNode.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
