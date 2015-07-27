package org.crustee.raft.storage.btree;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Condition;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.ComparableComparator;
import org.junit.Test;

public class LockFreeBTreeTest {

    public static final int COUNT = 100_000;

//    static String previousValue;
//
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
        Map<String, String> expectedValue = singletonMap("v", "v");
        bTree.insert("foo", expectedValue);

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.columns).has(Utils.numberOfElementsAndNulls(1));

        assertThat(bTree.root.keys[0]).isEqualTo("foo");
        assertThat(bTree.root.columns[0].asMap()).isEqualTo(expectedValue);
        assertThat(bTree.root.children).isEmpty();

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_insert_several_values() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(32);
        inject(bTree, 1, 2, 3);

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(3));
        assertThat(bTree.root.columns).has(Utils.numberOfElementsAndNulls(3));

        for (int i = 1; i <= 3; i++) {
            assertThat(bTree.root.keys[i - 1]).isEqualTo(i);
            assertThat(bTree.root.columns[i - 1].asMap()).isEqualTo(singletonMap("v" + i, "v" + i));
            assertThat(bTree.root.children).isEmpty();
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_insert_preserving_order() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(32);
        inject(bTree, 3, 1, 4, 2);

        assertThat(bTree.root.children).isEmpty();
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(4));
        assertThat(bTree.root.columns).has(Utils.numberOfElementsAndNulls(4));

        for (int i = 1; i <= 4; i++) {
            assertThat(bTree.root.keys[i - 1]).isEqualTo(i);
            assertThat(bTree.root.columns[i - 1].asMap()).isEqualTo(singletonMap("v" + i, "v" + i));
            assertThat(bTree.root.children).isEmpty();
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_find_in_root() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        inject(bTree, 3, 1, 4, 2);

        Row row = bTree.get(2);
        assertThat(row.asMap()).isEqualTo(singletonMap("v2", "v2"));

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_find_deeper() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        inject(bTree, 3, 1, 4, 2, 5, 13, 9);

        Row row = bTree.get(13);
        assertThat(row.asMap()).isEqualTo(singletonMap("v13", "v13"));

        bTree.forceVerifyInvariants();
    }

    private void inject(LockFreeBTree bTree, int... values) {
        for (int value : values) {
            bTree.insert(value, singletonMap("v" + value, "v" + value));
        }
    }

    @Test
    public void should_split_when_full() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(4);
        inject(bTree, 30, 10, 40);
        verifyContainsAll(bTree, Arrays.asList(10, 30, 40));
        inject(bTree, 20);

        inject(bTree, 60);
        verifyContainsAll(bTree, Arrays.asList(10, 20, 30, 40, 60));
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.columns).has(Utils.numberOfElementsAndNulls(1));
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(2));

        assertThat(bTree.root.keys[0]).isEqualTo(30);
        assertThat(bTree.root.columns[0].asMap()).isEqualTo(singletonMap("v30", "v30"));

        LockFreeBTree.Node left = bTree.root.children[0];
        LockFreeBTree.Node right = bTree.root.children[1];

        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(2));
        assertThat(left.columns).has(Utils.numberOfElementsAndNulls(2));
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(2));
        assertThat(right.columns).has(Utils.numberOfElementsAndNulls(2));
        assertThat(left.isLeaf()).isTrue();
        assertThat(right.isLeaf()).isTrue();

        assertThat(left.keys).startsWith(10, 20);
        assertThat(left.columns).is(containingMapsWith("v10", "v20"));

        assertThat(right.keys).startsWith(40, 60);
        assertThat(right.columns).is(containingMapsWith("v40", "v60"));

        inject(bTree, 70);
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(40, 60, 70);
        assertThat(right.columns).has(Utils.numberOfElementsAndNulls(3)).is(containingMapsWith("v40", "v60", "v70"));

        // should trigger a split of right node
        inject(bTree, 50);
        verifyContainsAll(bTree, Arrays.asList(10, 20, 30, 40, 60, 70, 50));
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(3));
        assertThat(bTree.root.children[0]).isEqualTo(left); // not changed
        LockFreeBTree.Node middle = bTree.root.children[1];
        right = bTree.root.children[2];
        assertThat(middle.keys).has(Utils.numberOfElementsAndNulls(2)).startsWith(40, 50);
        assertThat(middle.columns).has(Utils.numberOfElementsAndNulls(2)).is(containingMapsWith("v40", "v50"));
        assertThat(right.keys).has(Utils.numberOfElementsAndNulls(1)).startsWith(70);
        assertThat(right.columns).has(Utils.numberOfElementsAndNulls(1)).is(containingMapsWith("v70"));

        inject(bTree, 45);
        middle = bTree.root.children[1];
        assertThat(middle.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(40, 45, 50);
        assertThat(middle.columns).has(Utils.numberOfElementsAndNulls(3)).is(containingMapsWith("v40", "v45", "v50"));

        inject(bTree, 15);
        left = bTree.root.children[0];
        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(10, 15, 20);
        assertThat(left.columns).has(Utils.numberOfElementsAndNulls(3)).is(containingMapsWith("v10", "v15", "v20"));

        // shoudl spit left, we will have 3 keys in root and 4 children
        inject(bTree, 25);
        assertThat(bTree.root.keys).has(Utils.numberOfElementsAndNulls(3)).startsWith(20, 30, 60);
        assertThat(bTree.root.columns).has(Utils.numberOfElementsAndNulls(3)).is(containingMapsWith("v20", "v30", "v60"));
        assertThat(bTree.root.children).has(Utils.numberOfElementsAndNulls(4));

        left = bTree.root.children[0];
        assertThat(left.keys).has(Utils.numberOfElementsAndNulls(2)).startsWith(10, 15);
        assertThat(left.columns).has(Utils.numberOfElementsAndNulls(2)).is(containingMapsWith("v10", "v15"));
        assertThat(left.children).has(Utils.numberOfElementsAndNulls(0));
        LockFreeBTree.Node middleLeft = bTree.root.children[1];
        assertThat(middleLeft.keys).has(Utils.numberOfElementsAndNulls(1)).startsWith(25);
        assertThat(middleLeft.columns).has(Utils.numberOfElementsAndNulls(1)).is(containingMapsWith("v25"));
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
                bTree.insert(key, singletonMap(value, value));
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
        integers.forEach(i -> bTree.insert(i, singletonMap("v" + i, "v" + i)));

        assertThat(bTree.getCount()).isEqualTo(COUNT);
        checkLockFreeBTreeConsistency(bTree.root);

        verifyContainsAll(bTree, integers);

        int correctFind = 0;
        for (Integer i : integers) {
            assertThat(bTree.get(i).asMap()).describedAs("correct find : " + correctFind).isEqualTo(singletonMap("v" + i, "v" + i));
            correctFind++;
        }

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_replace() throws Exception {
        LockFreeBTree bTree = new LockFreeBTree(16);

        List<Integer> integers = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        Collections.shuffle(integers, new Random(44));
        integers.forEach(i -> bTree.insert(42, singletonMap("v" + 42, "v" + 42)));

        assertThat(bTree.getCount()).isEqualTo(1);
        checkLockFreeBTreeConsistency(bTree.root);

        assertThat(bTree.get(42).asMap()).isEqualTo(singletonMap("v42", "v42"));

        bTree.forceVerifyInvariants();
    }

    @Test
    public void should_apply_function_in_sort_order() {
        LockFreeBTree bTree = new LockFreeBTree(8);

        List<Integer> integers = IntStream.range(0, 500).boxed().collect(Collectors.toList());
        Collections.shuffle(integers, new Random(57));
        integers.forEach(i -> bTree.insert(i, singletonMap("v" + i, "v" + i)));

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

    private Condition<? super Row[]> containingMapsWith(String ... entries) {
        return new Condition<Row[]>() {
            @Override
            public boolean matches(Row[] rows) {
                for (int i = 0; i < entries.length; i++) {
                    String entry = entries[i];
                    assertThat(rows[i].asMap()).isEqualTo(singletonMap(entry, entry));
                }
                return true;
            }
        };
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

    AtomicInteger nodeCount = new AtomicInteger();

    private void checkLockFreeBTreeConsistency(LockFreeBTree.Node node) {
        nodeCount.incrementAndGet();
        checkOrdered(node.keys, ComparableComparator.get());
        checkValues(node.keys, node.columns);

        if (node.hasChildren()) {
            for (Object child : node.children) {
                if (child != null) {
                    checkLockFreeBTreeConsistency((LockFreeBTree.Node) child);
                }
            }
        }
    }

    private static void checkOrdered(Object[] array, Comparator<Object> comparator) {
        for (int i = 0; i < array.length - 1; i++) {
            Object first = array[i];
            Object second = array[i + 1];
            if (first == null | second == null) {
                return;
            }
            assertThat(comparator.compare(first, second)).isLessThan(0);
        }
    }

    private static void checkValues(Object[] keys, Row[] values) {
        assertThat(keys.length).isEqualTo(values.length);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            if (key == null) {
                return;
            }
            assertThat(values[i].asMap()).isEqualTo(singletonMap("v" + key, "v" + key));
        }
    }

}