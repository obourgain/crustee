package org.crustee.raft.storage.sstable.index;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.bloomfilter.bitset.DirectByteBufferFactory;
import org.crustee.raft.utils.ByteBufferUtils;
import com.carrotsearch.hppc.IntArrayList;
import uk.co.real_logic.agrona.UnsafeAccess;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

// TODO use ints or long for pointer to index ? or said differently, should be allow an index larger than 2 GB ?
class FlatIndexSummary implements IndexSummary {

    private final int samplingInterval;
    private final ByteBuffer memory;
    // TODO convert to off heap
    // TODO inline in the memory
    @VisibleForTesting
    final IntArrayList positions;

    private FlatIndexSummary(ByteBuffer memory, IntArrayList positions, int samplingInterval) {
        this.memory = memory;
        this.positions = positions;
        this.samplingInterval = samplingInterval;
    }

    @Override
    public int previousIndexEntryLocation(ByteBuffer key) {
        int keyPosition = binarySearch(0, positions.size(), key, ByteBufferUtils.lengthFirstComparator());
        if(keyPosition >= 0) {
            // found !
            return (int) pointerToIndexEntry(keyPosition);
        }
        // -1 to reverse the binary search negative value which is offset by one to handle the 0 case,
        // and another -1 to take the flooring entry
        return (int) pointerToIndexEntry(- keyPosition - 1 - 1);
    }

    // JDK binary search without range check
    @SuppressWarnings("Duplicates")
    private int binarySearch(int fromIndex, int toIndex, ByteBuffer key, Comparator<ByteBuffer> c) {
        ByteBuffer currentKey = DirectByteBufferFactory.allocateEmpty();
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int start = positions.get(mid);
            int length = keyLength(mid, start);

            ByteBuffer midVal = DirectByteBufferFactory.slice(memory, currentKey, start, length);

            int cmp = c.compare(midVal, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    private int keyLength(int index, int start) {
        if (index == positions.size() - 1) {
            // long is the size of the pointer to the index file
            return memory.capacity() - start - Long.BYTES;
        }
        return positions.get(index + 1) - start - Long.BYTES;
    }

    @VisibleForTesting
    ByteBuffer keyAt(int index) {
        assert index <= positions.size();
        int start = positions.get(index);
        int length = keyLength(index, start);
        return ((ByteBuffer) memory
                .duplicate()
                .position(start)
                .limit(start + length))
                .slice();
    }

    @VisibleForTesting
    long pointerToIndexEntry(int index) {
        assert index <= positions.size();
        int start = positions.get(index);
        int length = keyLength(index, start);
        return memory.getLong(start + length); // long = pointer
    }

    @Override
    public int getSamplingInterval() {
        return samplingInterval;
    }

    private static int presizeEstimation(MmapIndexReader indexReader, int samplingInterval) {
        // TODO use max key size
        return Math.max((int) (indexReader.indexFileSize / samplingInterval), 512);
    }

    static Builder builder(MmapIndexReader indexReader, int samplingInterval) {
        return new Builder(indexReader, samplingInterval);
    }

    static class Builder {

        private final MmapIndexReader indexReader;
        private final int samplingInterval;

        private final IntArrayList entryPositions = new IntArrayList();
        // first free position in the buffer
        private int memoryPosition = 0;

        // use UnsafeBuffer because of better API, but we may reimplement this stuff in some helper class
        private UnsafeBuffer memory;

        Builder(MmapIndexReader indexReader, int samplingInterval) {
            this.indexReader = indexReader;
            this.samplingInterval = samplingInterval;

            int presizingEstimate = presizeEstimation(indexReader, samplingInterval);
            long address = UnsafeAccess.UNSAFE.allocateMemory(presizingEstimate);
            ByteBuffer byteBuffer = DirectByteBufferFactory.wrap(address, presizingEstimate);
            this.memory = new UnsafeBuffer(byteBuffer);
        }

        private void load() {
            assert memoryPosition == 0;
            assert entryPositions.isEmpty();
            // TODO record number of entries in index file to create correctly sized lists
            indexReader.iterate(this::filter, this::callback);
        }

        private boolean filter(ByteBuffer byteBuffer, int integer) {
            return integer % samplingInterval == 0;
        }

        private void callback(ByteBuffer key, int position) {
            assert key.position() == 0;
            checkEnoughRoom(key);
            int keySize = key.limit();
            // point to the start of the entry in the summary
            entryPositions.add(memoryPosition);
            memory.putBytes(memoryPosition, key, keySize);
            memoryPosition += keySize;
            memory.putLong(memoryPosition, position);
            memoryPosition += Long.BYTES;
        }

        private void checkEnoughRoom(ByteBuffer key) {
            int currentCapacity = memory.capacity();
            int currentFree = currentCapacity - memoryPosition;
            int requiredStorage = key.limit() + Long.BYTES; // long is position in index file
            if (currentFree <= requiredStorage) {
                long address = memory.addressOffset();
                // TODO do better estimate based on what has already been read, or maybe with an histogram of key sizes in index file ?
                int presizingEstimate = presizeEstimation(indexReader, samplingInterval);
                int newSize = currentCapacity + presizingEstimate;
                long newAddress = UnsafeAccess.UNSAFE.reallocateMemory(address, newSize);
                DirectByteBufferFactory.wrap(memory.byteBuffer(), newAddress, newSize);
            }
        }

        private void truncate() {
            // release unused memory at the end of the buffer
            long address = memory.addressOffset();
            long newAddress = UnsafeAccess.UNSAFE.reallocateMemory(address, memoryPosition);
            DirectByteBufferFactory.wrap(memory.byteBuffer(), newAddress, memoryPosition);
            // rewrap the buffer so the UnsafeBuffer can update its capacity
            memory.wrap(memory.byteBuffer());
        }

        FlatIndexSummary build() {
            load();
            truncate();
            return new FlatIndexSummary(memory.byteBuffer(), entryPositions, samplingInterval);
        }
    }

}
