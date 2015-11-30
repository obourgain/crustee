package org.crustee.raft.storage.sstable;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assert;
import org.junit.Test;

public class TemporaryMetadataBufferSlabTest {

    @Test
    public void should_allocate_memory_of_requested_size() throws Exception {
        int elementCount = 10;
        int elementSize = 12;
        BufferingChannel.TemporaryMetadataBufferSlab slab = new BufferingChannel.TemporaryMetadataBufferSlab(elementCount, elementSize);

        assertThat(slab.slab.capacity()).isEqualTo(elementCount * elementSize);

        assertThat(slab.buffers).hasSize(elementCount);
        assertThat(slab.index).isEqualTo(0);

        ByteBuffer[] buffers = slab.buffers;
        for (int i = 0; i < buffers.length; i++) {
            ByteBuffer buffer = buffers[i];
            assertThat(buffer.position()).describedAs("at position %d", i).isEqualTo(0);
            assertThat(buffer.limit()).describedAs("at position %d", i).isEqualTo(elementSize);
        }
    }

    @Test
    public void should_get_one_element() throws Exception {
        int elementCount = 10;
        int elementSize = 12;
        BufferingChannel.TemporaryMetadataBufferSlab slab = new BufferingChannel.TemporaryMetadataBufferSlab(elementCount, elementSize);

        ByteBuffer byteBuffer = slab.get();
        assertThat(byteBuffer.position()).isEqualTo(0);
        assertThat(byteBuffer.limit()).isEqualTo(elementSize);
        assertThat(slab.index).isEqualTo(1);
    }

    @Test
    public void should_get_to_clean_state_on_reset() throws Exception {
        int elementCount = 10;
        int elementSize = 12;
        BufferingChannel.TemporaryMetadataBufferSlab slab = new BufferingChannel.TemporaryMetadataBufferSlab(elementCount, elementSize);

        for (int i = 0; i < 6; i++) {
            slab.get();
        }

        slab.reset();

        assertThat(slab.index).isEqualTo(0);
        checkElements(slab.buffers, Buffer::position, b -> b.isEqualTo(0));
    }

    @Test
    public void should_clear_on_close() throws Exception {
        int elementCount = 10;
        int elementSize = 12;
        BufferingChannel.TemporaryMetadataBufferSlab slab = new BufferingChannel.TemporaryMetadataBufferSlab(elementCount, elementSize);

        slab.close();

        checkElements(slab.buffers, Function.identity(), Assert::isNull);
    }

    private <T, E> void checkElements(T[] array, Function<T, E> extracting, Consumer<AbstractObjectAssert> condition) {
        for (int i = 0; i < array.length; i++) {
            condition.accept(assertThat(extracting.apply(array[i]))
                    .describedAs("at index %s", i));
        }
    }
}