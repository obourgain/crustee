package org.crustee.raft.storage.write;

import static java.lang.Long.MAX_VALUE;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.Memtable;
import org.crustee.raft.storage.sstable.SSTableReader;
import org.crustee.raft.storage.sstable.SSTableWriter;
import org.crustee.raft.storage.table.CrusteeTable;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;

public class MemtableHandler implements EventHandler<WriteEvent>, LifecycleAware {

    private static final Logger logger = getLogger(MemtableHandler.class);

    private ExecutorService flushMemtableExecutor = new ThreadPoolExecutor(1, 4, MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>(10),
            r -> {
                Thread thread = new Thread(r);
                thread.setName("memtable-flush-thread");
                thread.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());
                return thread;
            });

    private Memtable memtable;
    private final CrusteeTable table;

    public MemtableHandler(CrusteeTable table) {
        this.table = table;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        event.getRowKey().position(0);
        memtable.insert(event.getRowKey(), event.getValues());
        if (sequence % 10_000_000 == 0) {
            logger.info("inserted event {}", sequence);
        }
        if (memtable.getCount() >= 1_000_000) {
            logger.info("flushing memtable");
            memtable.freeze();
            Memtable oldMemtable = memtable;
            flushMemtableExecutor.submit(() -> writeSSTable(oldMemtable, table));
            newMemtable();
            logger.info("created memtable");
        }
    }

    private void writeSSTable(Memtable memtable, CrusteeTable crusteeTable) {
        long start = System.currentTimeMillis();
        Path tablePath = UncheckedIOUtils.tempFile();
        Path indexPath = UncheckedIOUtils.tempFile();
        try (SSTableWriter ssTableWriter = new SSTableWriter(tablePath, indexPath, memtable)) {
            ssTableWriter.write();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        crusteeTable.memtableFlushed(memtable, new SSTableReader(tablePath, indexPath));
        logger.info("flush memtable duration {}", (end - start) + " for sstable " + tablePath + " index " + indexPath);
    }

    @Override
    public void onStart() {
        newMemtable();
    }

    private void newMemtable() {
        Memtable memtable = new LockFreeBTreeMemtable();
        logger.info("created memtable " + System.identityHashCode(memtable));
        table.registerMemtable(memtable);
        this.memtable = memtable;
    }

    @Override
    public void onShutdown() {

    }
}
