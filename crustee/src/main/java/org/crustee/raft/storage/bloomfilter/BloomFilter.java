package org.crustee.raft.storage.bloomfilter;

import static org.crustee.raft.storage.bloomfilter.Descriptor.HashFunction.CHRONICLE_XX_HASH;
import static org.slf4j.LoggerFactory.getLogger;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.crustee.raft.storage.bloomfilter.bitset.BitSet;
import org.crustee.raft.storage.bloomfilter.bitset.UnsafeNativeAccessor;
import org.crustee.raft.utils.UncheckedIOUtils;
import org.slf4j.Logger;
import net.openhft.hashing.LongHashFunction;
import uk.co.real_logic.agrona.UnsafeAccess;

public final class BloomFilter implements MutableBloomFilter {

    private static final Logger logger = getLogger(BloomFilter.class);

    private final Descriptor descriptor;

    private final LongHashFunction longHashFunction;
    private final LongHashFunction longHashFunction2;
    private final BitSet bitSet;

    private BloomFilter(Descriptor descriptor, BitSet bitSet) {
        this.descriptor = descriptor;
        this.bitSet = bitSet;
        this.longHashFunction = LongHashFunction.xx_r39(descriptor.getSeed1());
        this.longHashFunction2 = LongHashFunction.xx_r39(descriptor.getSeed2());
    }

    private BloomFilter(long seed1, long seed2, LongHashFunction longHashFunction, LongHashFunction longHashFunction2, int hashFunctionCount, BitSet bitSet) {
        this.descriptor = new Descriptor(CHRONICLE_XX_HASH, CHRONICLE_XX_HASH, seed1, seed2, hashFunctionCount, bitSet.sizeInBytes());
        this.longHashFunction = longHashFunction;
        this.longHashFunction2 = longHashFunction2;
        this.bitSet = bitSet;
    }

    public static BloomFilter create(long elementCount, double falsePositiveProbability) {
        long requiredSizeInBits = requiredSizeInBits(elementCount, falsePositiveProbability);
        long requiredSizeInBytes = requiredSizeInBits / Byte.SIZE;

        assert requiredSizeInBytes <= Integer.MAX_VALUE;
        int requiredSizeInBytes_int = (int) requiredSizeInBytes;

        int hashFunctionCount = hashFunctionCount(elementCount, requiredSizeInBits);

        ByteAccessorFactory.newAccessor(requiredSizeInBytes_int);
        long address = UnsafeAccess.UNSAFE.allocateMemory(requiredSizeInBytes_int);
        BitSet bitSet = new BitSet(new UnsafeNativeAccessor(address, requiredSizeInBytes_int));

        logger.debug("Created bloom filter with {} bits for {} elements, {} hash functions and fp proba {}",
                requiredSizeInBytes_int * 8, elementCount, hashFunctionCount, falsePositiveProbability);
        long seed = 42;
        long seed2 = 4722;
        return new BloomFilter(seed, seed2, LongHashFunction.xx_r39(seed), LongHashFunction.xx_r39(seed2), hashFunctionCount, bitSet);
    }

    /**
     * Computes the maximum number of hash functions according to http://billmill.org/bloomfilter-tutorial/,
     * which is based on http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
     */
    private static int hashFunctionCount(long elementCount, long bitCount) {
        // alias parameters have consistent naming with the classic formulas
        long m = bitCount;
        long n = Math.max(elementCount, 1); // avoid division by 0 if is 0 passed as elementCount
        long k = Math.round((m / n) * Math.log(2));
        assert k <= Integer.MAX_VALUE;
        return (int) k;
    }

    /**
     * Formula from https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    private static long requiredSizeInBits(long elementCount, double falsePositiveProbability) {
        // alias parameters have consistent naming with the classic formulas
        double p = falsePositiveProbability;
        long n = elementCount;
        // bit count according to http://hur.st/bloomfilter
        long m = (long) Math.ceil(-(n * Math.log(p) / (Math.pow(Math.log(2), 2))));
        return m;
    }

    private static long indexFromHashes(long h1, long h2, int i, long size) {
        long hash = h1 + i * h2;
        long negbit = hash >> 63;
        return ((hash ^ negbit) - negbit) % size;
    }

    public boolean mayBePresent(ByteBuffer key) {
        long h1 = longHashFunction.hashBytes(key);
        long h2 = longHashFunction2.hashBytes(key);
        long bitSetSize = bitSet.sizeInBits();
        for (int i = 0; i < descriptor.getHashFunctionCount(); i++) {
            long index = indexFromHashes(h1, h2, i, bitSetSize);
            if (!bitSet.get(index)) {
                // return as soon as one bit is missing
                return false;
            }
        }
        return true;
    }

    /**
     * A buffer ready to read.
     */
    @Override
    public void add(ByteBuffer buffer) {
        // per doc, the method does not modify the buffer
        long h1 = longHashFunction.hashBytes(buffer);
        long h2 = longHashFunction2.hashBytes(buffer);
        long bitSetSize = bitSet.sizeInBits();
        for (int i = 0; i < descriptor.getHashFunctionCount(); i++) {
            long index = indexFromHashes(h1, h2, i, bitSetSize);
            bitSet.set(index);
        }
    }

    public long writeTo(WritableByteChannel channel) {
        long written = UncheckedIOUtils.write(channel, descriptor.asBuffer());
        written += bitSet.writeTo(channel);
        return written;
    }

    public BloomFilter readFrom(ReadableByteChannel channel) {
        Descriptor descriptor = Descriptor.fromBuffer(ByteBuffer.allocate(Descriptor.BUFFER_SIZE));
        BitSet bitSet = BitSet.readFrom(channel);
        return new BloomFilter(descriptor, bitSet);
    }

}
