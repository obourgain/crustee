package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
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
        // capture current state
        Tables localTables = this.tables;
        Iterator<ReadOnlyMemtable> memtableIterator = localTables.memtables.iterator();
        Iterator<SSTableReader> ssTablesIterator = localTables.ssTableReaders.iterator();

        TreeMap<ByteBuffer, ByteBuffer> values = new TreeMap<>();

        // read first in sstables so we can easily overwrite with data from memtables
        boolean foundInSSTables = searchInSSTables(key, ssTablesIterator, values);

        boolean foundInMemtables = searchInMemtables(key, memtableIterator, values);
        return foundInMemtables | foundInSSTables ? values : null;
    }

    private boolean searchInMemtables(ByteBuffer key, Iterator<ReadOnlyMemtable> memtableIterator, TreeMap<ByteBuffer, ByteBuffer> values) {
        boolean entryFound = false;
        while(memtableIterator.hasNext()) {
            ReadOnlyMemtable memtable = memtableIterator.next();
            Row row = memtable.get(key);
            if(row != null) {
                entryFound = true;
                values.putAll(row.asMap());
            }
        }
        return entryFound;
    }

    private boolean searchInSSTables(ByteBuffer key, Iterator<SSTableReader> ssTablesIterator, TreeMap<ByteBuffer, ByteBuffer> values) {
        boolean entryFound = false;
        while(ssTablesIterator.hasNext()) {
            SSTableReader ssTableReader = ssTablesIterator.next();

            Optional<Row> row = ssTableReader.get(key);
            row.map(Row::asMap).ifPresent(values::putAll);
            entryFound |= row.isPresent();
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
        if(closed) {
            throw new IllegalStateException("Table have already been closed");
        }
        Tables currentTables = this.tables;
        // allow the GC to release stuff
        this.tables = new Tables();
        // This may cause exceptions about the sstables being closed, but we don't care
        // as the table will be put offline, so we are only breaking read that occure at the same time as the shutdown.
        currentTables.close();
        closed = true;
    }

    protected static class Tables {

        protected final List<ReadOnlyMemtable> memtables;
        protected final List<SSTableReader> ssTableReaders;

        private Tables(List<ReadOnlyMemtable> memtables, List<SSTableReader> ssTableReaders) {
            this.memtables = memtables;
            this.ssTableReaders = ssTableReaders;
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
            newMemtables.sort(Timestamped.TIMESTAMPED_COMPARATOR);
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
