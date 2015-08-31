package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.SSTableReader;

public class CrusteeTable {

    // memtable and sstables are sorted from older to most recent
    // so we can simply put relevant columns from every table to a map
    // and older values will be overwritten
    protected final CopyOnWriteArrayList<ReadOnlyMemtable> memtables = new CopyOnWriteArrayList<>();
    protected final CopyOnWriteArrayList<SSTableReader> ssTableReaders = new CopyOnWriteArrayList<>();

    public SortedMap<ByteBuffer, ByteBuffer> get(ByteBuffer key) {
        // take the snapshots early to have the most possible view on the tables
        // there is a quite benign race here, if a memtable is flushed and the corresponding sstable is
        // added between the two lines, we may end up searching in both the memtable and sstable
        // corresponding to the same data
        // We could assign a UUID to each memtable and sstable and deduplicate based rads with a set, but before adding this
        // we should evaluate and try to measure the real probability of seeing this double read, and
        // what is the real performance impact
        Iterator<ReadOnlyMemtable> memtableIterator = memtables.iterator();
        Iterator<SSTableReader> ssTablesIterator = ssTableReaders.iterator();

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

    public void registerMemtable(ReadOnlyMemtable memtable) {
        memtables.add(memtable);
    }

    public void memtableFlushed(ReadOnlyMemtable memtable, SSTableReader ssTableReader) {
        // add the new sstable before removing the memtable to avoid having a window where the data may not be found
        ssTableReaders.add(ssTableReader);
        memtables.remove(memtable);
    }

}
