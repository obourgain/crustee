package org.crustee.raft.storage.write;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.crustee.raft.storage.commitlog.Segment;
import org.crustee.raft.storage.memtable.LockFreeBTreeMemtable;
import org.crustee.raft.storage.memtable.ReadOnlyMemtable;
import org.crustee.raft.storage.memtable.WritableMemtable;
import org.crustee.raft.storage.sstable.SSTableWriter;
import org.crustee.raft.storage.table.CrusteeTable;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import com.lmax.disruptor.EventHandler;

public class MemtableHandler implements EventHandler<WriteEvent> {

    private static final Logger logger = getLogger(MemtableHandler.class);

    private final CrusteeTable table;
    private final ExecutorService flushMemtableExecutor;
    private final int maxEvents;
    protected WritableMemtable memtable;
    private Segment currentSegment;

    public MemtableHandler(CrusteeTable table, ExecutorService flushMemtableExecutor, Segment segment, int maxEvents) {
        this.table = table;
        this.flushMemtableExecutor = flushMemtableExecutor;
        // We need to manage the first segment differently as we won't get it through write events.
        // Write events only carry a segment if it changes from the previous event, not if it is the first.
        this.maxEvents = maxEvents;
        this.currentSegment = segment;
        segment.acquire();
        newMemtable();
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        assert event.getRowKey().position() == 0 : "Event's key position is not 0 " + event.getRowKey().position();
        memtable.insert(event.getRowKey(), event.getValues());
        if (event.getSegment() != null) {
            Segment segment = event.getSegment();
            memtable.addSegment(segment);
            segment.release(); // paired with acquire() in the handler that adds segments to the event
        }
        if (memtable.getCount() >= maxEvents) {
            flushMemtable();
        }
    }

    private void flushMemtable() {
        logger.info("flushing memtable");
        memtable.freeze();
        ReadOnlyMemtable oldMemtable = memtable;
        flushMemtableExecutor.execute(() -> writeSSTable(oldMemtable, table));
        newMemtable();
        logger.info("created memtable");
    }

    private void writeSSTable(ReadOnlyMemtable memtable, CrusteeTable crusteeTable) {
        long start = System.currentTimeMillis();
        Path tablePath = UncheckedIOUtils.tempFile();
        Path indexPath = UncheckedIOUtils.tempFile();
        try (SSTableWriter ssTableWriter = new SSTableWriter(tablePath, indexPath, memtable)) {
            ssTableWriter.write();
            long end = System.currentTimeMillis();
            crusteeTable.memtableFlushed(memtable, ssTableWriter.toReader());
            logger.info("flush memtable duration {} ms for sstable {}, index {}", (end - start), tablePath, indexPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            memtable.close();
        }
    }

    private void newMemtable() {
        // TODO OBO create a synchronized clock
        // it is safe to call currentTimeMillis as long a we have :
        // - only one instance generating it
        // - no more than one call per millis (or more, depending on the timer granularity
        WritableMemtable memtable = new LockFreeBTreeMemtable(System.currentTimeMillis());
        logger.info("created memtable " + System.identityHashCode(memtable));
        table.registerMemtable(memtable);
        memtable.addSegment(currentSegment);
        this.memtable = memtable;
    }

}
