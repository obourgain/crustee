package org.crustee.raft.storage.bloomfilter;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class BloomFilterTest {

    @Test
    public void should_be_present() throws Exception {
        double falsePositiveProbability = 0.01d;
        int elementCount = 1_000_000;
        BloomFilter bloomFilter = BloomFilter.create(elementCount, falsePositiveProbability);

        List<ByteBuffer> keys = IntStream.range(0, elementCount)
                .mapToObj(i -> ByteBuffer.allocate(8).putInt(0, i).putInt(4, i + 42))
                .collect(Collectors.toList());
        Collections.shuffle(keys, new Random(187));

        for (ByteBuffer byteBuffer : keys) {
            bloomFilter.add(byteBuffer);
        }

        for (ByteBuffer byteBuffer : keys) {
            assertThat(bloomFilter.mayBePresent(byteBuffer)).isTrue();
        }

        List<ByteBuffer> notPresents = IntStream.range(0, elementCount)
                .mapToObj(i -> ByteBuffer.allocate(8).putInt(0, i).putInt(4, i * 1000))
                .collect(Collectors.toList());
        int fpCount = 0;

        for (ByteBuffer notPresent : notPresents) {
            if (bloomFilter.mayBePresent(notPresent)) {
                fpCount++;
            }
        }

        assertThat(fpCount).isLessThan((int) (elementCount * falsePositiveProbability * 1.5)); // * 1.5 to add some margin
    }

}