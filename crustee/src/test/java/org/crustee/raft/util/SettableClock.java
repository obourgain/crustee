package org.crustee.raft.util;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class SettableClock extends Clock {

    private long current;

    public SettableClock(long current) {
        this.current = current;
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.systemDefault();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(current);
    }

    public void setTime(long time) {
        current = time;
    }
}
