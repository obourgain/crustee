package org.crustee.raft.storage.bloomfilter;

import static java.util.Objects.requireNonNull;
import java.nio.ByteBuffer;
import com.google.common.base.Preconditions;

class Descriptor {

    private static final int CURRENT_VERSION = 1;
    protected static final int BUFFER_SIZE =
            4 + /* version */
                    4 + 4 + 8 + 8 + /* hash functions type + seed */
                    4 + /* hash function count */
                    4 /* bitset size in bytes */
    ;

    private final int version;
    private final HashFunction hashFunction1;
    private final HashFunction hashFunction2;
    private final long seed1;
    private final long seed2;
    private final int hashFunctionCount;
    private final int bitSetSizeIntBytes;

    Descriptor(HashFunction hashFunction1, HashFunction hashFunction2, long seed1, long seed2, int hashFunctionCount, int bitSetSizeIntBytes) {
        this(CURRENT_VERSION, hashFunction1, hashFunction2, seed1, seed2, hashFunctionCount, bitSetSizeIntBytes);
    }

    private Descriptor(int version, HashFunction hashFunction1, HashFunction hashFunction2, long seed1, long seed2, int hashFunctionCount, int bitSetSizeIntBytes) {
        this.version = version;
        this.hashFunction1 = requireNonNull(hashFunction1);
        this.hashFunction2 = requireNonNull(hashFunction2);
        this.seed1 = seed1;
        this.seed2 = seed2;
        this.hashFunctionCount = hashFunctionCount;
        this.bitSetSizeIntBytes = bitSetSizeIntBytes;
    }

    enum HashFunction {
        CHRONICLE_XX_HASH(0);

        final int code;

        HashFunction(int code) {
            this.code = code;
        }

        static HashFunction fromCode(int code) {
            for (HashFunction func : values()) {
                if (func.code == code) {
                    return func;
                }
            }
            throw new IllegalStateException("Unknown hash function code: " + code);
        }
    }

    public ByteBuffer asBuffer() {
        return (ByteBuffer) ByteBuffer.allocate(BUFFER_SIZE)
                .putInt(version)
                .putInt(hashFunction1.code)
                .putInt(hashFunction2.code)
                .putLong(seed1)
                .putLong(seed2)
                .putInt(hashFunctionCount)
                .putInt(bitSetSizeIntBytes)
                .flip();
    }

    /**
     * Will consume up to {@link #BUFFER_SIZE} bytes from the buffer, and thus mutate its position.
     */
    public static Descriptor fromBuffer(ByteBuffer buffer) {
        Preconditions.checkState(buffer.remaining() >= BUFFER_SIZE, "expected to read %s bytes, but %s remaining");
        return new Descriptor(
                buffer.getInt(),
                HashFunction.fromCode(buffer.getInt()),
                HashFunction.fromCode(buffer.getInt()),
                buffer.getLong(),
                buffer.getLong(),
                buffer.getInt(),
                buffer.getInt()
        );
    }

    public HashFunction getHashFunction1() {
        return hashFunction1;
    }

    public HashFunction getHashFunction2() {
        return hashFunction2;
    }

    public long getSeed1() {
        return seed1;
    }

    public long getSeed2() {
        return seed2;
    }

    public int getHashFunctionCount() {
        return hashFunctionCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Descriptor that = (Descriptor) o;

        if (version != that.version) return false;
        if (seed1 != that.seed1) return false;
        if (seed2 != that.seed2) return false;
        if (hashFunctionCount != that.hashFunctionCount) return false;
        if (bitSetSizeIntBytes != that.bitSetSizeIntBytes) return false;
        if (hashFunction1 != that.hashFunction1) return false;
        return hashFunction2 == that.hashFunction2;

    }

    @Override
    public int hashCode() {
        int result = version;
        result = 31 * result + hashFunction1.hashCode();
        result = 31 * result + hashFunction2.hashCode();
        result = 31 * result + (int) (seed1 ^ (seed1 >>> 32));
        result = 31 * result + (int) (seed2 ^ (seed2 >>> 32));
        result = 31 * result + hashFunctionCount;
        result = 31 * result + bitSetSizeIntBytes;
        return result;
    }

    @Override
    public String toString() {
        return "Descriptor{" +
                "version=" + version +
                ", hashFunction1=" + hashFunction1 +
                ", hashFunction2=" + hashFunction2 +
                ", seed1=" + seed1 +
                ", seed2=" + seed2 +
                ", hashFunctionCount=" + hashFunctionCount +
                ", bitSetSizeIntBytes=" + bitSetSizeIntBytes +
                '}';
    }
}
