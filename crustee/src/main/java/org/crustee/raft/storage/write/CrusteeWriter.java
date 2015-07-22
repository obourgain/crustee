package org.crustee.raft.storage.write;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.crustee.raft.storage.commitlog.CommitLog;
import org.slf4j.Logger;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.Util;
import uk.co.real_logic.agrona.BitUtil;

public class CrusteeWriter {

    private static final Logger logger = getLogger(CrusteeWriter.class);

    public static final int BENCH_COUNT = 50 * 1000 * 1000;
    public static final int WARMUP_COUNT = 100_000;
    public static final int KEY_SIZE = 16;
    public static final int VAlUE_SIZE = 200;

    // see https://github.com/LMAX-Exchange/disruptor/wiki/Getting-Started

    public static void main(String[] args) throws InterruptedException {
        CommitLog commitLog = new CommitLog();

        Executor executor = Executors.newCachedThreadPool();

        WriteEventFactory factory = new WriteEventFactory();

        int bufferSize = Util.ceilingNextPowerOfTwo(20 * 1024);
//        int bufferSize = 1024;
        assert BitUtil.isPowerOfTwo(bufferSize);
        logger.info("using ring buffer of size {}", bufferSize);

        Disruptor<WriteEvent> disruptor = new Disruptor<>(factory, bufferSize, executor, ProducerType.MULTI, new BlockingWaitStrategy());

        RingBuffer<WriteEvent> ringBuffer = disruptor.getRingBuffer();
        CommitLogWriteHandler commitLogWriteHandler = new CommitLogWriteHandler(commitLog);
        CommitLogFSyncHandler commitLogFSyncHandler = new CommitLogFSyncHandler(commitLog);
        MemtableHandler memtableHandler = new MemtableHandler();

        disruptor
                .handleEventsWith(commitLogWriteHandler)
                .then(commitLogFSyncHandler)
                .then(memtableHandler);

        disruptor.start();

        WriteEventProducer producer = new WriteEventProducer(ringBuffer);

        for (long l = 0; l < WARMUP_COUNT; l++) {
            ByteBuffer key = ByteBuffer.allocate(KEY_SIZE);
            ByteBuffer value = ByteBuffer.allocate(VAlUE_SIZE);
            key.putLong(0, l);
            value.putLong(0, l);
            producer.onWriteRequest(key, value);
        }
        try {
            MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("did some warmup, now let's go for {} inserts", BENCH_COUNT);
        long start = System.currentTimeMillis();
        for (long l = 0; l < BENCH_COUNT; l++) {
            ByteBuffer key = ByteBuffer.allocate(randomKeySize());
            ByteBuffer value = ByteBuffer.allocate(randomValueSize());
            key.putLong(0, l);
            value.putLong(0, l);
            producer.onWriteRequest(key, value);
        }
        long end = System.currentTimeMillis();
        System.out.println("duration: " + (end - start));

        disruptor.shutdown();

        System.exit(0);
    }

    private static int randomKeySize() {
        double random = Math.random();
        int result = (int) (8 * random) - 4 + KEY_SIZE;
        return result;
    }

    private static int randomValueSize() {
        double random = Math.random();
        int result = (int) (80 * random) - 40 + VAlUE_SIZE;
        return result;
    }

}
