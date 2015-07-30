package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.List;
import org.crustee.raft.storage.memtable.Memtable;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.storage.sstable.KVLocation;
import org.crustee.raft.storage.sstable.SSTableReader;

public class CrusteeTable {

    private final Memtable memtable;
    private final List<SSTableReader> readers; // TODO these are not thread safe

    public CrusteeTable(Memtable memtable, List<SSTableReader> readers) {
        this.memtable = memtable;
        this.readers = readers;
    }

    public Row get(ByteBuffer key) {
        Row value = memtable.get(key);
        if (value != null) {
            return value;
        }
        for (SSTableReader reader : readers) {
            KVLocation localisation = reader.findKVLocalisation(key);
            if (localisation.isFound()) {
                value = reader.get(localisation);
                assert value != null : "value not found in table but present in index " + localisation;
                return value;
            }
        }
        return null;
    }


}
