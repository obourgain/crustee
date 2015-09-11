package org.crustee.raft.storage.bloomfilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.crustee.raft.storage.bloomfilter.Descriptor.HashFunction.CHRONICLE_XX_HASH;
import java.nio.ByteBuffer;
import org.junit.Test;

public class DescriptorTest {

    @Test
    public void serialize_deserialize() throws Exception {
        Descriptor expected = new Descriptor(CHRONICLE_XX_HASH,
                CHRONICLE_XX_HASH,
                42, 129,
                3,
                10_000
        );
        ByteBuffer byteBuffer = expected.asBuffer();

        Descriptor actual = Descriptor.fromBuffer(byteBuffer);
        assertThat(actual).isEqualTo(expected);
    }
}