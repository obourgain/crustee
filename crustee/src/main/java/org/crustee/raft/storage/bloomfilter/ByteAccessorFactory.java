package org.crustee.raft.storage.bloomfilter;

import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.bloomfilter.bitset.ByteAccessor;
import org.crustee.raft.storage.bloomfilter.bitset.ByteBufferAccessor;
import org.crustee.raft.storage.bloomfilter.bitset.UnsafeNativeAccessor;
import org.slf4j.Logger;
import com.google.common.base.Joiner;
import uk.co.real_logic.agrona.UnsafeAccess;

public class ByteAccessorFactory {

    private static final Logger logger = getLogger(ByteAccessorFactory.class);

    private static final String PROVIDER_PROPERTY = "byteaccessor.provider";
    private static final String PROVIDER_NAME = System.getProperty(PROVIDER_PROPERTY, Provider.direct_byte_buffer.name());
    private static final Provider PROVIDER = Provider.fromString(PROVIDER_NAME);

    public static ByteAccessor newAccessor(int size) {
        return PROVIDER.create(size);
    }

    @VisibleForTesting
    public static ByteAccessor newAccessorFromRandomProvider(int size) {
        double v = Math.random() * Provider.values().length;
        Provider provider = Provider.values()[((int) v)];
        logger.trace("using provider {}", provider);
        return provider.create(size);
    }

    private enum Provider {
        heap_byte_buffer {
            @Override
            ByteAccessor create(int size) {
                ByteBuffer buffer = ByteBuffer.allocate(size);
                return new ByteBufferAccessor(buffer);
            }
        },
        direct_byte_buffer {
            @Override
            ByteAccessor create(int size) {
                ByteBuffer buffer = ByteBuffer.allocateDirect(size);
                return new ByteBufferAccessor(buffer);
            }
        },
        unsafe_memory {
            @Override
            ByteAccessor create(int size) {
                long address = UnsafeAccess.UNSAFE.allocateMemory(size);
                return new UnsafeNativeAccessor(address, size);
            }
        };

        abstract ByteAccessor create(int size);

        static Provider fromString(String s) {
            for (Provider provider : values()) {
                if (provider.name().equals(s)) {
                    logger.info("Using {} ByteAccessor provider", provider);
                    return provider;
                }
            }
            throw new IllegalStateException("No ByteAccessor provider found with name '" + s + "', valid values are " + Joiner.on(", ").join(values()));
        }
    }
}
