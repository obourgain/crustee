package org.crustee.raft;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class LogEntry {
    long index;
    String key;
    String value;
    long term;
}
