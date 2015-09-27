package org.crustee.raft.storage.write;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.crustee.raft.storage.commitlog.SegmentFactory;
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

    public static final int BENCH_COUNT = 10 * 1000 * 1000;
    public static final int WARMUP_COUNT = 100_000;
    public static final int KEY_SIZE = 16;
    public static final int VAlUE_SIZE = 200;

    // see https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started

    public static void main(String[] args) throws InterruptedException {
        CommitLog commitLog = new CommitLog(new SegmentFactory(128*1024*1024));

        Executor executor = Executors.newCachedThreadPool();

        WriteEventFactory factory = new WriteEventFactory();

        int bufferSize = Util.ceilingNextPowerOfTwo(2 * 1024);
        assert BitUtil.isPowerOfTwo(bufferSize);
        logger.info("using ring buffer of size {}", bufferSize);

        Disruptor<WriteEvent> disruptor = new Disruptor<>(factory, bufferSize, executor, ProducerType.MULTI, new BlockingWaitStrategy());

        CrusteeTable crusteeTable = new CrusteeTable();

        RingBuffer<WriteEvent> ringBuffer = disruptor.getRingBuffer();
        CommitLogWriteHandler commitLogWriteHandler = new CommitLogWriteHandler(commitLog, 1024 * 1024, 1024);
        CommitLogFSyncHandler commitLogFSyncHandler = new CommitLogFSyncHandler(commitLog, 1024 * 1024, 1024);
        MemtableHandler memtableHandler = new MemtableHandler(crusteeTable, new ThreadPoolExecutor(1, 4, 1, MINUTES, new LinkedBlockingQueue<>(1000)), 1_000_000);

        disruptor
                .handleEventsWith(commitLogWriteHandler)
                .then(commitLogFSyncHandler)
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
        long start = System.currentTimeMillis();
        for (long l = 0; l < BENCH_COUNT; l++) {
            publishEvent(producer, l);
        }
        long end = System.currentTimeMillis();
        System.out.println("duration: " + (end - start));

        disruptor.shutdown();

        try {
            // let some time to flush
            SECONDS.sleep(20);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("warming up reader");
        for (long l = 0; l < 100; l++) {
            long s = System.currentTimeMillis();
            blackhole = crusteeTable.get(ByteBuffer.allocate(KEY_SIZE).putLong(0, l));
            long e = System.currentTimeMillis();
            System.out.println((e - s) + " ms");
        }
        logger.info("done warming");

        try {
            SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        int i = BENCH_COUNT / 100;
        logger.info("start reading {} ", i);
        for (long l = 0; l < i; l++) {
            blackhole = crusteeTable.get(ByteBuffer.allocate(KEY_SIZE).putLong(0, l));
        }
        logger.info("done reading");

        System.exit(0);
    }

    static volatile Object blackhole;

    private static void publishEvent(WriteEventProducer producer, long l) {
        int keySize = randomKeySize();
        int valueSize = randomValueSize();
        ByteBuffer command = ByteBuffer.allocate(keySize + valueSize);
        ByteBuffer key = (ByteBuffer) command.duplicate().limit(keySize);
        ByteBuffer columnValue = ByteBuffer.allocate(valueSize);
        Map<ByteBuffer, ByteBuffer> value = Collections.singletonMap(ByteBuffer.allocate(keySize), columnValue);
        key.putLong(0, l);
        columnValue.putLong(0, l);
        producer.onWriteRequest(command, key, value);
    }

    private static int randomKeySize() {
        double random = Math.random();
        int result = (int) (8 * random) - 4 + KEY_SIZE;
        return result;
//        return KEY_SIZE;
    }

    private static int randomValueSize() {
        double random = Math.random();
        int result = (int) (80 * random) - 40 + VAlUE_SIZE;
        return result;
    }

}
