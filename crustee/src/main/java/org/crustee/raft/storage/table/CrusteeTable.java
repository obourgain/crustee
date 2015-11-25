package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.SSTableReader;

public class CrusteeTable implements AutoCloseable {

    protected volatile Tables tables = new Tables();
    private volatile boolean closed = false;

    public SortedMap<ByteBuffer, ByteBuffer> get(ByteBuffer key) {
        ensureOpen();

        // Capture current state to avoid multiple volatile reads
        Tables localTables = this.tables;

        // Read first in sstables so we can easily overwrite with data from memtables
        TreeMap<ByteBuffer, ByteBuffer> values = new TreeMap<>();
        boolean foundInSSTables = searchInSSTables(key, localTables.ssTableReaders, values);
        boolean foundInMemtables = searchInMemtables(key, localTables.memtables, values);

        return foundInMemtables || foundInSSTables ? values : null;
    }

    private boolean searchInSSTables(ByteBuffer key, ArrayList<SSTableReader> ssTableReaders, TreeMap<ByteBuffer, ByteBuffer> values) {
        boolean entryFound = false;
        for (int i = 0, s = ssTableReaders.size(); i < s; i++) {
            SSTableReader ssTableReader = ssTableReaders.get(i);
            Row row = ssTableReader.get(key);
            if (row != null) {
                values.putAll(row.asMap());
                entryFound = true;
            }
        }
        return entryFound;
    }

    private boolean searchInMemtables(ByteBuffer key, ArrayList<ReadOnlyMemtable> memtables, TreeMap<ByteBuffer, ByteBuffer> values) {
        boolean entryFound = false;
        for (int i = 0, s = memtables.size(); i < s; i++) {
            ReadOnlyMemtable memtable = memtables.get(i);
            Row row = memtable.get(key);
            if (row != null) {
                entryFound = true;
                values.putAll(row.asMap());
            }
        }
        return entryFound;
    }

    public synchronized void registerMemtable(ReadOnlyMemtable memtable) {
        ensureOpen();
        this.tables = this.tables.with(memtable);
    }

    public synchronized void memtableFlushed(ReadOnlyMemtable memtable, SSTableReader ssTableReader) {
        ensureOpen();
        this.tables = this.tables.memtableFlushed(memtable, ssTableReader);
    }

    private void ensureOpen() {
        if(closed) {
            throw new IllegalStateException("Table have been closed");
        }
    }

    @Override
    public synchronized void close() throws Exception {
        ensureOpen();

        Tables currentTables = this.tables;
        // allow the GC to release stuff
        this.tables = new Tables();
        // This may cause exceptions about the sstables being closed, but we don't care
        // as the table will be put offline, so we are only breaking read that occure at the same time as the shutdown.
        currentTables.close();
        closed = true;
    }

    protected static class Tables {

        protected final ArrayList<ReadOnlyMemtable> memtables;
        protected final ArrayList<SSTableReader> ssTableReaders;

        private Tables(List<ReadOnlyMemtable> memtables, List<SSTableReader> ssTableReaders) {
            this.memtables = new ArrayList<>(memtables);
            this.ssTableReaders = new ArrayList<>(ssTableReaders);
        }

        private Tables() {
            this.memtables = new ArrayList<>();
            this.ssTableReaders = new ArrayList<>();
        }

        Tables with(ReadOnlyMemtable newMemtable) {
            List<ReadOnlyMemtable> newMemtables = new ArrayList<>(this.memtables);
            newMemtables.add(newMemtable);
            newMemtables.sort(Timestamped.TIMESTAMPED_COMPARATOR);
            return new Tables(newMemtables, this.ssTableReaders);
        }

        Tables memtableFlushed(ReadOnlyMemtable memtable, SSTableReader newReader) {
            List<SSTableReader> newReaders = new ArrayList<>(this.ssTableReaders);
            newReaders.add(newReader);
            newReaders.sort(Timestamped.TIMESTAMPED_COMPARATOR);
            List<ReadOnlyMemtable> newMemtables = new ArrayList<>(this.memtables);
            newMemtables.remove(memtable);
            return new Tables(newMemtables, newReaders);
        }

        @Override
        public String toString() {
            return "Tables{" +
                    "memtables=" + memtables +
                    ", ssTableReaders=" + ssTableReaders +
                    '}';
        }

        public void close() {
            ssTableReaders.forEach(SSTableReader::close);
        }
    }

}
