package org.crustee.raft.storage.bloomfilter.bitset;

import static java.lang.Math.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.Test;
import uk.co.real_logic.agrona.UnsafeAccess;

public class BitSetTest {

    @Test
    public void should_return_size() throws Exception {
        BitSet bitSet = new BitSet(new ByteBufferAccessor(ByteBuffer.allocate(Integer.BYTES)));
        assertThat(bitSet.sizeInBits()).isEqualTo(Integer.SIZE);
        assertThat(bitSet.sizeInBytes()).isEqualTo(Integer.BYTES);
    }

    @Test
    public void should_set_bits() throws Exception {
        int sizeInBytes = 2000;
        long address = UnsafeAccess.UNSAFE.allocateMemory(sizeInBytes);
        BitSet bitSet = new BitSet(new UnsafeNativeAccessor(address, sizeInBytes));

        Random random = new Random(778870);
        List<Integer> indices = IntStream.range(0, 1000)
                .map(i -> abs((int) (random.nextInt() % bitSet.sizeInBits())))
                .boxed()
                .collect(toList());

        indices.forEach(bitSet::set);

        for (Integer index : indices) {
            assertThat(bitSet.get(index)).isTrue();
        }

        indices.forEach(bitSet::clear);

        for (Integer index : indices) {
            assertThat(bitSet.get(index)).isFalse();
        }
    }

}