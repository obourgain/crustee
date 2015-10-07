package org.crustee.raft.storage.btree;

import java.util.Arrays;
import java.util.Comparator;

public class ArrayUtils {

    public static <T> T lastNonNullElement(T[] array) {
        return array[lastNonNullElementIndex(array)];
    }

    public static <T> int lastNonNullElementIndex(T[] array) {
        // a property of BTress is that node are at least half full (except root), so we will go faster starting from the end
        // (assuming efficient prefetchers for backward iteration)
        for (int i = array.length - 1; i >= 0; i--) {
            if (array[i] != null) {
                return i;
            }
        }
        throw new IllegalStateException("no non null element found in " + Arrays.toString(array));
    }

    public static void shiftOneStep(Object[] array, int from) {
        shift(array, from, 1);
    }

    public static void shiftBothOneStep(Object[] array1, Object[] array2, int from) {
        shift(array1, from, 1);
        shift(array2, from, 1);
    }

    public static void shift(Object[] array, int from, int steps) {
        assert from + steps < array.length;
        System.arraycopy(array, from, array, from + steps, array.length - (from + 1));
        Arrays.fill(array, from, from + steps, null);
    }

    public static void copyRangeToStartOfOther(Object[] src, Object[] target, int from, int count) {
        System.arraycopy(src, from, target, 0, count);
    }

    public static void copyWhole(Object[] src, Object[] target) {
        assert src.length == target.length;
        System.arraycopy(src, 0, target, 0, src.length);
    }

    public static int findIndexOfInstance(Object[] array, Object searched) {
        for (int i = 0; i < array.length; i++) {
            Object o = array[i];
            if (o == searched) {
                return i;
            }
        }
        return -1;
    }

    /**
     * returns :
     * - positive, the index where the value is
     * - Integer.MIN_VALUE, not found
     * - negative, not found but may be in children, at index where to search
     */
    public static <T> int linearSearch(T[] array, T key, Comparator<T> comparator) {
        return linearSearch(array, 0, array.length, key, comparator);
    }

    public static <T> int linearSearch(T[] array, int from, int to, T key, Comparator<T> comparator) {
        assert to <= array.length;
        int i;
        for (i = from; i < to; i++) {
            T o = array[i];
            if (o == null) {
                return -i - 1;
            }
            int compare = comparator.compare(key, o);
            if (compare < 0) {
                // should have been here
                return -i - 1;
            } else if (compare == 0) {
                return i;
            }
        }
        return -i - 1;
    }

    // JDK binary search without range check
    public static <T> int binarySearch(T[] a, int fromIndex, int toIndex, T key, Comparator<T> c) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = a[mid];
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

    public static boolean hasGaps(Object[] array) {
        for (int i = 0; i < array.length - 1; i++) {
            if (array[i] == null & array[i + 1] != null) {
                return true;
            }
        }
        return false;
    }

    public static int occupancy(Object[] array) {
        return (int) Arrays.asList(array).stream()
                .filter(o -> o != null)
                .count();
    }

}
