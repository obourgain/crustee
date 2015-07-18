package org.crustee.raft.rpc;

import java.util.List;
import org.crustee.raft.LogEntry;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AppendEntries implements Rpc {
    long term;
    String leaderId;
    long prevLogIndex;
    long prevLogTerm;
    List<LogEntry> entries;
    long leaderCommit;
    String sourceNodeId;
    String targetNodeId;

    public LogEntry lastEntry() {
        if(entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }
}
