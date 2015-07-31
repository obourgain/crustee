package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.crustee.raft.storage.memtable.Memtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.KVLocation;
import org.crustee.raft.storage.sstable.SSTableReader;

public class CrusteeTable {

    // memtable and sstables are sorted from older to most recent
    // so we can simply put relevant columns from every table to a map
    // and older values will be overwritten
    private final CopyOnWriteArrayList<Memtable> memtables = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SSTableReader> ssTableReaders = new CopyOnWriteArrayList<>();

    public SortedMap<ByteBuffer, ByteBuffer> get(ByteBuffer key) {
        // take the snapshots early to have the most possible view on the tables
        // there is a quite benign race here, if a memtable is flushed and the corresponding sstable is
        // added between the two lines, we may end up searching in both the memtable and sstable
        // corresponding to the same data
        // We could assign a UUID to each memtable and sstable and deduplicate based rads with a set, but before adding this
        // we should evaluate and try to measure the real probability of seeing this double read, and
        // what is the real performance impact
        Iterator<Memtable> memtableIterator = memtables.iterator();
        Iterator<SSTableReader> ssTablesIterator = ssTableReaders.iterator();

        TreeMap<ByteBuffer, ByteBuffer> values = new TreeMap<>();

        searchInMemtables(key, memtableIterator, values);

        searchInSSTables(key, ssTablesIterator, values);
        return values;
    }

    private void searchInMemtables(ByteBuffer key, Iterator<Memtable> memtableIterator, TreeMap<ByteBuffer, ByteBuffer> values) {
        while(memtableIterator.hasNext()) {
            Memtable memtable = memtableIterator.next();
            Row row = memtable.get(key);
            if(row != null) {
                values.putAll(row.asMap());
            }
        }
    }

    private void searchInSSTables(ByteBuffer key, Iterator<SSTableReader> ssTablesIterator, TreeMap<ByteBuffer, ByteBuffer> values) {
        while(ssTablesIterator.hasNext()) {
            SSTableReader ssTableReader = ssTablesIterator.next();
            KVLocation location = ssTableReader.findKVLocalisation(key);
            if(location.isFound()) {
                Row row = ssTableReader.get(location);
                assert row != null : "row found in index should be present in the table";
                Map<ByteBuffer, ByteBuffer> map = row.asMap();
                values.putAll(map);
            }
        }
    }

    public void registerMemtable(Memtable memtable) {
        memtables.add(memtable);
    }

    public void memtableFlushed(Memtable memtable, SSTableReader ssTableReader) {
        // add the new sstable before removing the memtable to avoid having a window where the data may not be found
        ssTableReaders.add(ssTableReader);
        memtables.remove(memtable);
    }

}
