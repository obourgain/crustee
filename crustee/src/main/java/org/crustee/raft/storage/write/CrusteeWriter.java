package org.crustee.raft.storage.write;

import static java.nio.ByteBuffer.allocateDirect;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.LongStream;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.SegmentFactory;
import org.crustee.raft.storage.sstable.index.MmapIndexReader;
import org.crustee.raft.storage.table.CrusteeTable;
import org.slf4j.Logger;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;
import uk.co.real_logic.agrona.BitUtil;

public class CrusteeWriter {

    private static final Logger logger = getLogger(CrusteeWriter.class);

    private static final int WARMUP_COUNT = 100_000;
    private static final int BENCH_COUNT = 10 * 1000 * 1000 + WARMUP_COUNT;
    private static final int KEY_SIZE = 16;
    private static final int VAlUE_SIZE = 200;

    // see https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started

    public static void main(String[] args) throws InterruptedException {
        CommitLog commitLog = new CommitLog(new SegmentFactory(128 * 1024 * 1024));

        Executor executor = Executors.newCachedThreadPool();

        WriteEventFactory factory = new WriteEventFactory();

        int bufferSize = Util.ceilingNextPowerOfTwo(2 * 1024);
        assert BitUtil.isPowerOfTwo(bufferSize);
        logger.info("using ring buffer of size {}", bufferSize);

        Disruptor<WriteEvent> disruptor = new Disruptor<>(factory, bufferSize, executor, ProducerType.MULTI, new BlockingWaitStrategy());

        CrusteeTable crusteeTable = new CrusteeTable();

        RingBuffer<WriteEvent> ringBuffer = disruptor.getRingBuffer();
        CommitLogWriteHandler commitLogWriteHandler = new CommitLogWriteHandler(commitLog);
        CommitLogFSyncHandler commitLogFSyncHandler = new CommitLogFSyncHandler(commitLog, 1024 * 1024, 1024);
        ThreadPoolExecutor flushMemtableExecutor = new ThreadPoolExecutor(1, 4, 1, MINUTES, new LinkedBlockingQueue<>(1000), r -> {
            Thread thread = new Thread(r);
            thread.setUncaughtExceptionHandler((t, e) -> logger.error("", e));
            return thread;
        });
        MemtableHandler memtableHandler = new MemtableHandler(crusteeTable, flushMemtableExecutor, commitLog.getCurrentSegment(), 1_000_000);

        new ScheduledThreadPoolExecutor(1, (ThreadFactory) Thread::new)
                .scheduleAtFixedRate(commitLog::syncSegments, 1, 1, SECONDS);

        disruptor
                .handleEventsWith(commitLogWriteHandler)
//                .then(commitLogFSyncHandler)
                .then(memtableHandler);

        disruptor.start();

        WriteEventProducer producer = new WriteEventProducer(ringBuffer);

        for (long l = 0; l < WARMUP_COUNT; l++) {
            publishEvent(producer, l);
        }
        try {
            SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("did some warmup, now let's go for {} inserts", BENCH_COUNT);
        {
            long start = System.currentTimeMillis();
            for (long l = 0; l < BENCH_COUNT; l++) {
                publishEvent(producer, l);
            }
            long end = System.currentTimeMillis();
            System.out.println("duration: " + (end - start));
        }

        disruptor.shutdown();

        try {
            // let some time to flush
            SECONDS.sleep(6);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("warming up reader");
        LongStream.range(0, 20_000)
//                .parallel()
                .forEach(l -> {
                    long s = System.nanoTime();
                    ByteBuffer key = allocateDirect(KEY_SIZE);
                    SortedMap<ByteBuffer, ByteBuffer> row = crusteeTable.get(key.putLong(0, l));
                    blackhole = row;
                    long e = System.nanoTime();
                    System.out.println((e - s) + " ns, found " + (row != null));
                });
        logger.info("done warming");

        try {
            SECONDS.sleep(2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final LongAdder found = new LongAdder();
        final LongAdder notFound = new LongAdder();
        int i = BENCH_COUNT;

        {
            logger.info("start reading {} existing keys", i);
            long start = System.currentTimeMillis();
            LongStream.range(0, i)
//                    .parallel()
                    .forEach(l -> {
                        // this doesn't work with randomly sized keys, as we can't know what will be the size, maybe use a dedicated
                        // Random instance with same seed to reproduce ?
                        ByteBuffer key = allocateDirect(KEY_SIZE);
                        SortedMap<ByteBuffer, ByteBuffer> result = crusteeTable.get(key.putLong(0, l));
                        if(result == null) {
                            notFound.increment();
                        } else {
                            found.increment();
                        }
                        blackhole = result;
                    });

            long end = System.currentTimeMillis();
            long duration = end - start;
            logger.info("done reading in {} ms, found {}, not found {}", duration, found, notFound);
        }

        {
            logger.info("start reading {} non existing keys", i);
            long start = System.currentTimeMillis();
            LongStream.range(0, i)
//                    .parallel()
                    .forEach(l -> {
                        ByteBuffer key = allocateDirect(KEY_SIZE);
                        SortedMap<ByteBuffer, ByteBuffer> result = crusteeTable.get(key.putLong(0, Long.MAX_VALUE - l));
                if(result == null) {
                    notFound.increment();
                } else {
                    found.increment();
                }
                blackhole = result;
            });
            long end = System.currentTimeMillis();
            long duration = end - start;
            logger.info("done reading in {} ms, found {}, not found {}", duration, found, notFound);
        }

        System.out.println("MmapIndexReader.globalScannedEntries " + MmapIndexReader.globalScannedEntries);

        System.exit(0);
    }

    private static volatile Object blackhole;

    private static void publishEvent(WriteEventProducer producer, long l) {
        int keySize = randomKeySize();
        int valueSize = randomValueSize();
        ByteBuffer command = allocateDirect(keySize + valueSize);
        ByteBuffer key = (ByteBuffer) command.duplicate().limit(keySize);
        ByteBuffer columnValue = allocateDirect(valueSize);
        Map<ByteBuffer, ByteBuffer> value = Collections.singletonMap(allocateDirect(keySize), columnValue);
        key.putLong(0, l);
        columnValue.putLong(0, l);
        producer.onWriteRequest(command, key, value);
    }

    private static int randomKeySize() {
//        double random = Math.random();
//        int result = (int) (8 * random) - 4 + KEY_SIZE;
//        return result;
        return KEY_SIZE;
    }

    private static int randomValueSize() {
        double random = Math.random();
        int result = (int) (80 * random) - 40 + VAlUE_SIZE;
        return result;
    }

}
