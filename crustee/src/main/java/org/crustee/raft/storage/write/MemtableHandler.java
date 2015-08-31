package org.crustee.raft.storage.write;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
import com.lmax.disruptor.LifecycleAware;

public class MemtableHandler implements EventHandler<WriteEvent>, LifecycleAware {

    private static final Logger logger = getLogger(MemtableHandler.class);

    private List<Segment> segments;
    private WritableMemtable memtable;
    private final CrusteeTable table;
    private final ExecutorService flushMemtableExecutor;
    private final int maxEvents;

    public MemtableHandler(CrusteeTable table, ExecutorService flushMemtableExecutor, int maxEvents) {
        this.table = table;
        this.flushMemtableExecutor = flushMemtableExecutor;
        this.segments = new ArrayList<>();
        this.maxEvents = maxEvents;
    }

    @Override
    public void onEvent(WriteEvent event, long sequence, boolean endOfBatch) throws Exception {
        assert event.getRowKey().position() == 0 : "Event's key position is not 0 " + event.getRowKey().position();
        memtable.insert(event.getRowKey(), event.getValues());
        if (event.getSegment() != null) {
            segments.add(event.getSegment());
        }
        if (memtable.getCount() >= maxEvents) {
            flushMemtable();
        }
    }

    private void flushMemtable() {
        logger.info("flushing memtable");
        memtable.freeze();
        ReadOnlyMemtable oldMemtable = memtable;
        Segment currentsegment = this.segments.get(this.segments.size() - 1);
        List<Segment> segmentsToClose = this.segments.subList(0, this.segments.size() - 1);
        flushMemtableExecutor.submit(() -> writeSSTable(oldMemtable, table, segmentsToClose));
        newMemtable();
        this.segments = new ArrayList<>();
        this.segments.add(currentsegment);
        logger.info("created memtable");
    }

    private void writeSSTable(ReadOnlyMemtable memtable, CrusteeTable crusteeTable, List<Segment> segmentsToClose) {
        long start = System.currentTimeMillis();
        Path tablePath = UncheckedIOUtils.tempFile();
        Path indexPath = UncheckedIOUtils.tempFile();
        try (SSTableWriter ssTableWriter = new SSTableWriter(tablePath, indexPath, memtable)) {
            ssTableWriter.write();
            long end = System.currentTimeMillis();
            crusteeTable.memtableFlushed(memtable, ssTableWriter.toReader());
            logger.info("flush memtable duration {} for sstable {}, index {}", (end - start), tablePath, indexPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            segmentsToClose.forEach(Segment::close);
        }
    }

    @Override
    public void onStart() {
        newMemtable();
    }

    private void newMemtable() {
        // TODO OBO create a synchronized clock
        // it is safe to call currentTimeMillis as long a we have :
        // - only one instance generating it
        // - no more than one call per millis (or more, depending on the timer granularity
        WritableMemtable memtable = new LockFreeBTreeMemtable(System.currentTimeMillis());
        logger.info("created memtable " + System.identityHashCode(memtable));
        table.registerMemtable(memtable);
        this.memtable = memtable;
    }

    @Override
    public void onShutdown() {

    }
}
