package org.crustee.raft.storage.btree;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class LockFreeBTreeTest {

    public static final int COUNT = 100_000;

    static String previousValue;

//    @BeforeClass
//    public static void setup() {
//        previousValue = System.setProperty("btree.verify.invariants", "true");
//    }
//
//    @AfterClass
//    public static void restore() {
//        if(previousValue != null) {
//            System.setProperty("btree.verify.invariants", previousValue);
//        }
//    }

    @Test
    public void should_insert() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(32);
        bTree.insert("foo", "v");

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.values).has(Utils.numberOfElementsAndNulls(1));

        assertThat(bTree.root.keys[0]).isEqualTo("foo");
        assertThat(bTree.root.values[0]).isEqualTo("v");
        assertThat(bTree.root.children).isEmpty();

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_insert_several_values() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(32);
        bTree.insert(1, "v1");
        bTree.insert(2, "v2");
        bTree.insert(3, "v3");

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(3));
        assertThat(bTree.root.values).has(Utils.numberOfElementsAndNulls(3));

        for (int i = 0; i < 3; i++) {
            assertThat(bTree.root.keys[i]).isEqualTo(i + 1);
            assertThat(bTree.root.values[i]).isEqualTo("v" + (i + 1));
            assertThat(bTree.root.children).isEmpty();
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_insert_preserving_order() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(32);
        bTree.insert(3, "v3");
        bTree.insert(1, "v1");
        bTree.insert(4, "v4");
        bTree.insert(2, "v2");

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(4));
        assertThat(bTree.root.values).has(Utils.numberOfElementsAndNulls(4));

        for (int i = 0; i < 4; i++) {
            assertThat(bTree.root.keys[i]).isEqualTo(i + 1);
            assertThat(bTree.root.values[i]).isEqualTo("v" + (i + 1));
            assertThat(bTree.root.children).isEmpty();
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_find_in_root() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        bTree.insert(3, "v3");
        bTree.insert(1, "v1");
        bTree.insert(4, "v4");
        bTree.insert(2, "v2");

        Object value = bTree.get(2);
        assertThat(value).isEqualTo("v2");

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_find_deeper() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        bTree.insert(3, "v3");
        bTree.insert(1, "v1");
        bTree.insert(4, "v4");
        bTree.insert(2, "v2");
        bTree.insert(5, "v5");
        bTree.insert(13, "v13");
        bTree.insert(9, "v9");

        Object value = bTree.get(13);
        assertThat(value).isEqualTo("v13");

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_split_when_full() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        bTree.insert(30, "v30");
        bTree.insert(10, "v10");
        bTree.insert(40, "v40");
        verifyContainsAll(bTree, Arrays.asList(10, 30, 40));
        bTree.insert(20, "v20");

        bTree.insert(60, "v60");
        verifyContainsAll(bTree, Arrays.asList(10, 20, 30, 40, 60));
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.values).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(2));

        assertThat(bTree.root.keys[0]).isEqualTo(30);
        assertThat(bTree.root.values[0]).isEqualTo("v30");

        LockFreeBTree.Node left = bTree.root.children[0];
        LockFreeBTree.Node right = bTree.root.children[1];

        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(2));
        assertThat(left.values).has(Utils.numberOfElementsAndNulls(2));
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(2));
        assertThat(right.values).has(Utils.numberOfElementsAndNulls(2));
        assertThat(left.isLeaf()).isTrue();
        assertThat(right.isLeaf()).isTrue();

        assertThat(left.keys).startsWith(10, 20);
        assertThat(left.values).startsWith("v10", "v20");

        assertThat(right.keys).startsWith(40, 60);
        assertThat(right.values).startsWith("v40", "v60");

        bTree.insert(70, "v70");
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(40, 60, 70);
        assertThat(right.values).has(Utils.numberOfElementsAndNulls(3)).startsWith("v40", "v60", "v70");

        // should trigger a split of right node
        bTree.insert(50, "v50");
        verifyContainsAll(bTree, Arrays.asList(10, 20, 30, 40, 60, 70, 50));
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(3));
        assertThat(bTree.root.children[0]).isEqualTo(left); // not changed
        LockFreeBTree.Node middle = bTree.root.children[1];
        right = bTree.root.children[2];
        assertThat(middle.keys).has(Utils.numberOfElementsAndNulls(2)).startsWith(40, 50);
        assertThat(middle.values).has(Utils.numberOfElementsAndNulls(2)).startsWith("v40", "v50");
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(1)).startsWith(70);
        assertThat(right.values).has(Utils.numberOfElementsAndNulls(1)).startsWith("v70");

        bTree.insert(45, "v45");
        middle = bTree.root.children[1];
        assertThat(middle.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(40, 45, 50);
        assertThat(middle.values).has(Utils.numberOfElementsAndNulls(3)).startsWith("v40", "v45", "v50");

        bTree.insert(15, "v15");
        left = bTree.root.children[0];
        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(10, 15, 20);
        assertThat(left.values).has(Utils.numberOfElementsAndNulls(3)).startsWith("v10", "v15", "v20");

        // shoudl spit left, we will have 3 keys in root and 4 children
        bTree.insert(25, "v25");
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(20, 30, 60);
        assertThat(bTree.root.values).has(Utils.numberOfElementsAndNulls(3)).startsWith("v20", "v30", "v60");
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(4));

        left = bTree.root.children[0];
        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(2)).startsWith(10, 15);
        assertThat(left.values).has(Utils.numberOfElementsAndNulls(2)).startsWith("v10", "v15");
        assertThat(left.children).has(Utils.numberOfElementsAndNulls(0));
        LockFreeBTree.Node middleLeft = bTree.root.children[1];
        assertThat(middleLeft.keys).has(Utils.numberOfElementsAndNulls(1)).startsWith(25);
        assertThat(middleLeft.values).has(Utils.numberOfElementsAndNulls(1)).startsWith("v25");
        assertThat(middleLeft.children).has(Utils.numberOfElementsAndNulls(0));
        assertThat(bTree.root.children[2]).isSameAs(middle); // should not have changed
        assertThat(bTree.root.children[3]).isSameAs(right); // should not have changed

        bTree.forceVerifyInvariants();
        verifyContainsAll(bTree, Arrays.asList(10, 20, 30, 40, 50, 60, 70, 45, 15, 25));
    }

    @Test
    public void should_have_correct_counts() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(16);
        for (long l = 0; l < 300_000; l++) {
            ByteBuffer key = ByteBuffer.allocate(8);
            ByteBuffer value = ByteBuffer.allocate(8);
            key.putLong(0, l);
            value.putLong(0, l);
            try {
                bTree.insert(key, value);
            } catch (Exception e) {
                System.out.println("failed at " + l);
            }
        }
        assertThat(bTree.getCount()).isEqualTo(300_000);
        AtomicInteger count = new AtomicInteger();
        bTree.applyInOrder((k, v) -> count.incrementAndGet());
        assertThat(count.get()).isEqualTo(300_000);
    }

    @Test
    public void should_add_lot_of_stuff() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(16);

        List<Integer> integers = IntStream.range(0, COUNT).boxed().collect(Collectors.toList());
        Collections.shuffle(integers, new Random(44));
        integers.forEach(i -> bTree.insert(i, "v" + i));

        assertThat(bTree.getCount()).isEqualTo(COUNT);
        checkLockFreeBTreeConsistency(bTree.root);

        verifyContainsAll(bTree, integers);

        int correctFind = 0;
        for (Integer i : integers) {
            assertThat(bTree.get(i)).describedAs("correct find : " + correctFind).isEqualTo("v" + i);
            correctFind++;
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_replace() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(16);

        List<Integer> integers = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        Collections.shuffle(integers, new Random(44));
        integers.forEach(i -> bTree.insert(42, "v" + 42));

        assertThat(bTree.getCount()).isEqualTo(1);
        checkLockFreeBTreeConsistency(bTree.root);

        assertThat(bTree.get(42)).isEqualTo("v42");

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_apply_function_in_sort_order() {
        LockFreeBTree bTree = new LockFreeBTree(8);

        List<Integer> integers = IntStream.range(0, 500).boxed().collect(Collectors.toList());
        Collections.shuffle(integers, new Random(57));
        integers.forEach(i -> bTree.insert(i, "v" + i));

        bTree.forceVerifyInvariants();
        AtomicReference<Integer> previousKey = new AtomicReference<>(-1);
        AtomicInteger keysSeen = new AtomicInteger();
        bTree.applyInOrder((k, v) -> {
            Integer previous = previousKey.get();
            assertThat((Integer) k).isEqualTo(previous + 1);
            previousKey.set((Integer) k);
            keysSeen.incrementAndGet();
        });
        assertThat(keysSeen.get()).isEqualTo(integers.size());
    }

    private void verifyContainsAll(LockFreeBTree bTree, List<Integer> ints) {
        Set<Object> present = new HashSet<>();
        bTree.executeOnEachNode(node -> present.addAll(Arrays.asList(node.keys)));

        for (int i = 0; i < ints.size(); i++) {
            Integer expected = ints.get(i);
            if (!present.contains(expected)) {
                throw new AssertionError("at " + i + " we lost " + expected);
            }
        }
    }

    private static final LockFreeBTree.ComparatorComparable comparator = new LockFreeBTree.ComparatorComparable();

    AtomicInteger nodeCount = new AtomicInteger();

    private void checkLockFreeBTreeConsistency(LockFreeBTree.Node node) {
        nodeCount.incrementAndGet();
        checkOrdered(node.keys, comparator);
        checkValues(node.keys, node.values);

        if (node.hasChildren()) {
            for (Object child : node.children) {
                if (child != null) {
                    checkLockFreeBTreeConsistency((LockFreeBTree.Node) child);
                }
            }
        }
    }

    private void checkOrdered(Object[] array, Comparator<Object> comparator) {
        for (int i = 0; i < array.length - 1; i++) {
            Object first = array[i];
            Object second = array[i + 1];
            if (first == null | second == null) {
                return;
            }
            assertThat(comparator.compare(first, second)).isLessThan(0);
        }
    }

    private void checkValues(Object[] keys, Object[] values) {
        assertThat(keys.length).isEqualTo(values.length);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            if (key == null) {
                return;
            }
            assertThat(values[i]).isEqualTo("v" + key);
        }
    }

}