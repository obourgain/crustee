package org.crustee.raft.storage.write;

import static java.lang.Long.MAX_VALUE;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.btree.LockFreeBTree;
import org.crustee.raft.storage.sstable.SSTableWriter;
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

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        event.getKey().position(0);
        event.getValue().position(0);
        memtable.insert(event.getKey(), event.getValue());
        if (sequence % 10_000_000 == 0) {
            logger.info("inserted event {}", sequence);
        }
        if (memtable.getCount() >= 1_000_000) {
            logger.info("flushing memtable");
            memtable.freeze();
            Memtable oldMemtable = memtable;
            flushMemtableExecutor.submit(() -> writeSSTable(oldMemtable));
            memtable = newMemtable();
            logger.info("created memtable");
        }
    }

    private void writeSSTable(Memtable memtable) {
        long start = System.currentTimeMillis();
        Path tablePath = UncheckedIOUtils.tempFile();
        Path indexPath = UncheckedIOUtils.tempFile();
        try (SSTableWriter ssTableWriter = new SSTableWriter(tablePath, indexPath, memtable)) {
            ssTableWriter.write();
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        logger.info("flush memtable duration {}", (end - start) + " for sstable " + tablePath + " index " + indexPath);
    }

    @Override
    public void onStart() {
        memtable = newMemtable();
    }

    private Memtable newMemtable() {
        Memtable memtable = new LockFreeBTree(16);
        logger.info("created memtable " + System.identityHashCode(memtable));
        return memtable;
    }

    @Override
    public void onShutdown() {

    }
}
