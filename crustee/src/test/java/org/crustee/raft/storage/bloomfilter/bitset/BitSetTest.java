package org.crustee.raft.storage.bloomfilter.bitset;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import org.crustee.raft.storage.bloomfilter.ByteAccessorFactory;
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

    @Test
    public void should_serialize_deserialize() throws Exception {
            BitSet bitSet = new BitSet(ByteAccessorFactory.newAccessorFromRandomProvider(Integer.SIZE));

            ByteChannel channel = new ByteArrayBackedChannel();
            bitSet.writeTo(channel);
            BitSet read = BitSet.readFrom(channel);

            assertThat(read).isEqualTo(bitSet);
    }

    private static class ByteArrayBackedChannel implements ByteChannel {
        ByteArrayOutputStream array = new ByteArrayOutputStream();

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (array.size() == 0) {
                return -1;
            }
            byte[] bytes = array.toByteArray();
            int index = 0;
            while (dst.remaining() > 0 && index < bytes.length) {
                dst.put(bytes[index++]);
            }
            array = new ByteArrayOutputStream();
            for (int i = index; i < bytes.length; i++) {
                array.write(bytes[i]);
            }
            return index;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (src.remaining() == 0) {
                return -1;
            }
            int written = 0;
            while (src.remaining() > 0) {
                array.write(src.get());
                written++;
            }
            return written;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() throws IOException {

        }

    }

}