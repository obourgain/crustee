package org.crustee.raft.bootstrap;

import static java.lang.Long.MAX_VALUE;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.crustee.raft.settings.Settings;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.SegmentFactory;
import org.crustee.raft.storage.table.CrusteeTable;
import org.crustee.raft.storage.write.CommitLogFSyncHandler;
import org.crustee.raft.storage.write.CommitLogWriteHandler;
import org.crustee.raft.storage.write.MemtableHandler;
import org.crustee.raft.storage.write.WriteEvent;
import org.crustee.raft.storage.write.WriteEventFactory;
import org.crustee.raft.storage.write.WriteEventProducer;
import org.slf4j.Logger;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;

public class Bootstrap {

    private static final Logger logger = getLogger(Bootstrap.class);

    public void start() {
        Settings settings = loadSettings();

        WriteEventFactory factory = new WriteEventFactory();

        int ringbufferSize = settings.getRingbufferSize();
        int bufferSize = Util.ceilingNextPowerOfTwo(ringbufferSize);
        logger.info("using ring buffer of size {}", bufferSize);

        Executor executor = Executors.newCachedThreadPool();
        Disruptor<WriteEvent> disruptor = new Disruptor<>(factory, bufferSize, executor, ProducerType.MULTI, new BlockingWaitStrategy());


        int segmentSize = settings.getCommitLogSegmentSize();
        SegmentFactory segmentFactory = new SegmentFactory(segmentSize);
        CommitLog commitLog = new CommitLog(segmentFactory);
        CommitLogWriteHandler commitLogWriteHandler = createCommitLogWriteHandler(settings, commitLog);

        CommitLogFSyncHandler commitLogFSyncHandler = createCommitLogFSyncHandler(settings, commitLog);

        CrusteeTable crusteeTable = new CrusteeTable();
        MemtableHandler memtableHandler = createMemtableHandler(settings, crusteeTable);

        disruptor
                .handleEventsWith(commitLogWriteHandler)
                .then(commitLogFSyncHandler)
                .then(memtableHandler);

        disruptor.start();

        RingBuffer<WriteEvent> ringBuffer = disruptor.getRingBuffer();
        WriteEventProducer producer = new WriteEventProducer(ringBuffer);
    }

    private CommitLogWriteHandler createCommitLogWriteHandler(Settings settings, CommitLog commitLog) {
        int maxUncommitedEvents = settings.getCommitlogWriterMaxEventsBufferCount();
        int maxUncommitedSize = settings.getCommitlogWriterMaxEventsBufferSize();
        return new CommitLogWriteHandler(commitLog, maxUncommitedSize, maxUncommitedEvents);
    }

    private CommitLogFSyncHandler createCommitLogFSyncHandler(Settings settings, CommitLog commitLog) {
        int maxUnsyncedSize = settings.getCommitlogWriterMaxEventsUnsyncedSize();
        int maxUnsyncedCount = settings.getCommitlogWriterMaxEventsUnsyncedCount();
        return new CommitLogFSyncHandler(commitLog, maxUnsyncedSize, maxUnsyncedCount);
    }

    private MemtableHandler createMemtableHandler(Settings settings, CrusteeTable crusteeTable) {
        int minFlusherThreads = settings.getMemtableFlushThreadMin();
        int maxFlusherThreads = settings.getMemtableFlushThreadMax();
        ExecutorService flushMemtableExecutor = new ThreadPoolExecutor(minFlusherThreads, maxFlusherThreads, MAX_VALUE, TimeUnit.DAYS, new LinkedBlockingQueue<>(10),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("memtable-flush-thread");
                    Logger logger = getLogger(thread.getName());
                    thread.setUncaughtExceptionHandler((t, e) -> logger.error("", e));
                    return thread;
                });

        return new MemtableHandler(crusteeTable, flushMemtableExecutor, 1_000_000);
    }

    private Settings loadSettings() {
        Path path = Paths.get("target/classes/settings.yml");
        return new Settings(path);
    }

}
