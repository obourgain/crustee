package org.crustee.raft.storage.btree;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Comparator;
import org.junit.Test;

public class ArrayUtilsTest {

    @Test
    public void should_find_by_linear_search() throws Exception {
        Integer[] array = new Integer[]{1, 2, 3, 4};
        Comparator<Integer> compare = Integer::compare;
        int index = ArrayUtils.linearSearch(array, 2, compare);
        assertThat(index).isEqualTo(1);
    }

    @Test
    public void should_find_slot_to_insert_by_linear_search() throws Exception {
        Integer[] array = new Integer[]{1, 2, 3, 4};
        Comparator<Integer> compare = Integer::compare;
        int index = ArrayUtils.linearSearch(array, 10, compare);
        int binaryIndex = ArrayUtils.binarySearch(array, 0, array.length, 10, compare);
        assertThat(index).isEqualTo(binaryIndex);
    }

    @Test
    public void should_find_index_to_insert_out_of_bounds_by_linear_search() throws Exception {
        Integer[] array = new Integer[]{1, 2, 4, 5};
        Comparator<Integer> compare = Integer::compare;
        int index = ArrayUtils.linearSearch(array, 3, compare);
        int binaryIndex = ArrayUtils.binarySearch(array, 0, array.length, 3, compare);
        assertThat(index).isEqualTo(binaryIndex);
    }
}