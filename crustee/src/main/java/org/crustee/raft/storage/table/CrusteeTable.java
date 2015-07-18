package org.crustee.raft.storage.table;

import java.nio.ByteBuffer;
import java.util.List;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.sstable.KVLocalisation;
import org.crustee.raft.storage.sstable.SSTableReader;

public class CrusteeTable {

    private final Memtable memtable;
    private final List<SSTableReader> readers; // TODO these are not thread safe

    public CrusteeTable(Memtable memtable, List<SSTableReader> readers) {
        this.memtable = memtable;
        this.readers = readers;
    }

    public ByteBuffer get(ByteBuffer key) {
        ByteBuffer value = memtable.get(key);
        if (value != null) {
            return value;
        }
        for (SSTableReader reader : readers) {
            KVLocalisation localisation = reader.findKVLocalisation(key);
            if (localisation.isFound()) {
                value = reader.get(localisation);
                assert value != null : "value not found in table but present in index " + localisation;
                return value;
            }
        }
        return null;
    }



}
