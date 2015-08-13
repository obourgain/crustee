package org.crustee.raft.storage.sstable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.crustee.raft.storage.memtable.Memtable;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractSSTableTest {

    @FunctionalInterface
    interface Action {
        void run(SSTableWriter writer, File table, File index)
                throws IOException;
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    void test(Memtable memtable, Action... actions) throws IOException {

        File table = temporaryFolder.newFile();
        File index = temporaryFolder.newFile();

        try (SSTableWriter writer = new SSTableWriter(table.toPath(),
                index.toPath(), memtable)) {

            for (Action action : actions) {
                action.run(writer, table, index);
            }
        }
        finally {
            Files.deleteIfExists(table.toPath());
            Files.deleteIfExists(index.toPath());
        }
    }
}
