package org.crustee.raft.storage.sstable;

import java.io.File;
import java.io.IOException;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractSSTableTest {

    @SuppressWarnings("AccessCanBeTightened") // intellij is wrong here, do not make this private
    @FunctionalInterface
    protected interface Action {
        void run(SSTableWriter writer, File table, File index) throws IOException;
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected void test(WritableMemtable memtable, Action... actions) throws IOException {

        File table = temporaryFolder.newFile();
        File index = temporaryFolder.newFile();

        try (SSTableWriter writer = new SSTableWriter(table.toPath(),
                index.toPath(), memtable)) {

            for (Action action : actions) {
                action.run(writer, table, index);
            }
        } finally {
            UncheckedIOUtils.run(index::delete);
            UncheckedIOUtils.run(table::delete);
        }
    }

    protected void initSstable(SSTableWriter writer, File table, File index) {
        writer.write();
        new SSTableConsistencyChecker(table.toPath(), index.toPath(), Assert::fail).check();
    }

}
