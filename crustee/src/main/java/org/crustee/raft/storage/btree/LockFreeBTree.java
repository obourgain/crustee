package org.crustee.raft.storage.btree;

import static java.lang.String.format;
import static org.crustee.raft.storage.btree.ArrayUtils.binarySearch;
import static org.crustee.raft.storage.btree.ArrayUtils.copyRangeToStartOfOther;
import static org.crustee.raft.storage.btree.ArrayUtils.copyWhole;
import static org.crustee.raft.storage.btree.ArrayUtils.findIndexOfInstance;
import static org.crustee.raft.storage.btree.ArrayUtils.hasGaps;
import static org.crustee.raft.storage.btree.ArrayUtils.lastNonNullElement;
import static org.crustee.raft.storage.btree.ArrayUtils.linearSearch;
import static org.crustee.raft.storage.btree.ArrayUtils.occupancy;
import static org.crustee.raft.storage.btree.ArrayUtils.shiftBothOneStep;
import static org.crustee.raft.storage.btree.ArrayUtils.shiftOneStep;
import static uk.co.real_logic.agrona.UnsafeAccess.UNSAFE;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.Memtable;
import org.crustee.raft.storage.row.MapRow;
import org.crustee.raft.storage.row.Row;
import org.crustee.raft.utils.ComparableComparator;
import uk.co.real_logic.agrona.UnsafeAccess;

/**
 * Single writer/multi-reader lock free BTree.
 * Use copy on write techniques for node modifications.
 * <li>adding a new value in free space is done using atomic ops, setting the value first then the key to avoid a reader seeing the key but not yet the value</li>
 * <li>replacing a value is done with ordered put for value and then a volatile set on the value (may probable be avoided)</li>
 * <li>inserting a value copies the node, shift the keys, columns and children to make room, insert the new KV and replace the node with the copy in the parent
 * (or sets the root) with a volatile set</li>
 * <p>
 * Reader only go down in the tree and use volatile reads.
 * <p>
 * When a node is almost full (one slot left) we split the node, so we avoid the need to recursively split the parents by always having a free
 * slot in the nodes.
 * <p>
 * The structure is safe for writing by only one thread, and the method to traverse it like {@link Memtable#applyInOrder(BiConsumer)}
 * or {@link #executeOnEachNode(Consumer)} are unsafe to use when the BTree is still mutated.
 * The object may be frozen to indicate that no more mutations will happen by calling {@link #freeze()}, the tree may then be safely
 * traversed by an other thread.
 * If assertions are enabled, an {@link AssertionError} will be thrown if an operation requires to be done on the writer thread or
 * after a {@link #freeze()}.
 * <p>
 * Using a persistent datastructure would generate more garbage on inserts but avoid the need for volatile read (except the root)
 * and would allow snapshotting of the tree. Garbage generation would be limited by the maximum size of a memtable and aggressively
 * reusing keys/columns/children arrays, so it is worth trying.
 */
public class LockFreeBTree implements Memtable {

    private static final boolean AUTO_VERIFY_INVARIANTS_ENABLE = Boolean.getBoolean("btree.verify.invariants");
    private static final boolean USE_BINARY_SEARCH = false;

    private static final boolean ASSERTION_ENABLED;

    static {
        ASSERTION_ENABLED = LockFreeBTree.class.desiredAssertionStatus();
    }

    private static final Node[] EMPTY = {};

    private static final int base;
    private static final int shift;

    static {
        int scale = UNSAFE.arrayIndexScale(Object[].class);
        if ((scale & (scale - 1)) != 0) {
            throw new Error("data type scale not a power of two");
        }
        shift = 31 - Integer.numberOfLeadingZeros(scale);
        base = UnsafeAccess.UNSAFE.arrayBaseOffset(Object[].class);
    }

    private final Comparator<Object> comparator;

    private final ArrayStack<Node> pathStack = new ArrayStack<>();
    private final int size;
    private final int median; // used when splitting
    private boolean frozen = false;

    // we are single writer, so volatile is ok for those
    private volatile int count = 0;
    protected volatile Node root;

    private WeakReference<Thread> writerThread;

    public LockFreeBTree(int size) {
        this(ComparableComparator.get(), size);
    }

    public LockFreeBTree(Comparator comparator, int size) {
        this.comparator = comparator;
        this.size = size;
        this.median = size / 2;
        this.root = new Node(size, false);
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            writerThread = new WeakReference<>(Thread.currentThread());
        }
    }

    /**
     * The counter is incremented before the actual modification is visible, so the count may not be perfectly accurate
     */
    public int getCount() {
        return count;
    }

    public void freeze() {
        currentThreadIsWriter();
        this.frozen = true;
    }

    public <K1, K2, V> void insert(K1 rowKey, Map<K2, V> values) {
        assert !frozen;
        assert pathStack.isEmpty();
        currentThreadIsWriter();
        try {
            doInsert(rowKey, values, root);
        } finally {
            clearWriterPathStack();
        }
    }

    private void doInsert(Object rowKey, Map values, Node node) {
        verifyInvariants(node);
        if (node.isLeaf()) {
            // insert in this node
            node.insert(this, rowKey, values, comparator);
            return;
        }

        int i;
        for (i = 0; i < node.numberOfKeys(); i++) {
            Object k = node.keys[i];
            int compare = comparator.compare(rowKey, k);
            if (compare < 0) {
                // lesser, we go to the left child
                doInsertInChild(rowKey, values, node, i);
                return;
            }
            if (compare == 0) {
                node.set(rowKey, values, i);
                return;
            }
        }

        // greater, we go to the right child or set it here if leaf
        // don't add +1 to go to the right child because the loop already incremented i
        doInsertInChild(rowKey, values, node, i);
    }

    private void doInsertInChild(Object rowKey, Map values, Node node, int i) {
        Node tmp = node.fastChildAt(i, this);
        assert tmp != null;
        pathStack.push(node);
        doInsert(rowKey, values, tmp);
    }

    public <K> Row get(K key) {
        Node node = root;
        return doGet(key, node);
    }

    private <K> Row doGet(Object key, Node node) {
        verifyInvariants(node);
        if (node.isLeaf()) {
            int index = ArrayUtils.linearSearch(node.keys, key, comparator);
            if (index < 0) {
                return null;
            }
            return node.valueAt(index);
        }

        int i;
        for (i = 0; i < node.numberOfKeys(); i++) {
            Object k = node.keys[i];
            int compare = comparator.compare(key, k);
            if (compare < 0) {
                // lesser, we go to the left child
                Node child = node.childAt(i);
                Row value = doGet(key, child);
                return value;
            }
            if (compare == 0) {
                return node.valueAt(i);
            }
        }
        Node child = node.childAt(i);
        return doGet(key, child);
    }

    protected static class Node {
        protected final Node[] children;
        protected final Object[] keys;
        protected final Row[] columns;
        private int numberOfKeys;

        private Node(int size, boolean withChildren) {
            children = withChildren ? new Node[size + 1] : EMPTY;
            keys = new Object[size];
            columns = new Row[size];
            numberOfKeys = 0;
        }

        /**
         * Does not ensure thread safety, this must be used only before publication of the node
         */
        private void setChildrenAround(Node left, Node right, int index) {
            // does NOT copy, it should only be called from a method that copies
            assert hasChildren();
            children[index] = left;
            children[index + 1] = right;
        }

        // the BTree param is not pretty but avoids one pointer per Node if it was non static inner class
        public void insert(LockFreeBTree bTree, Object rowKey, Map values, Comparator<Object> comparator) {
            int searchIndex = search(keys, 0, numberOfKeys(), rowKey, comparator);
            Node node = insertAt(bTree, rowKey, values, comparator, searchIndex);
            bTree.verifyInvariants();
            if (node.isFull(bTree)) {
                split(bTree, node);
                bTree.verifyInvariants();
            }
        }

        // returns the new node if a copy have been created or this if changed in place
        private Node insertAt(LockFreeBTree bTree, Object rowKey, Map values, Comparator<Object> comparator, int searchIndex) {
            assert !isFull(bTree) : "should be split preemptively";
            int insertIndex;
            if (searchIndex < 0) { // this is the -(index + 1) where we should insert
                insertIndex = -searchIndex - 1;
                if (fastHasKeyAt(insertIndex, bTree)) {
                    // move stuff
                    Node newNode = copyShifted(insertIndex);
                    newNode.numberOfKeys++;
                    newNode.fastSet(rowKey, values, insertIndex, bTree);
                    bTree.count++;
                    if (bTree.pathStack.isEmpty()) {
                        // adding to root
                        bTree.root = newNode;
                    } else {
                        replaceInParent(bTree, this, newNode);
                    }
                    return newNode;
                } else {
                    this.set(rowKey, values, insertIndex);
                    numberOfKeys++;
                    bTree.count++;
                    return this;
                }
            } else {
                // overwrite existing
                assert keys[searchIndex] != null;
                assert comparator.compare(rowKey, keys[searchIndex]) == 0;
                this.set(rowKey, values, searchIndex);
                return this;
            }
        }

        private void replaceInParent(LockFreeBTree bTree, Node nodeToReplace, Node newNode) {
            Node parent = bTree.pathStack.peek();
            int indexOfInstance = findIndexOfInstance(parent.children, nodeToReplace);
            assert indexOfInstance >= 0;
            UNSAFE.putObjectVolatile(parent.children, byteOffset(indexOfInstance), newNode);
        }

        private static void split(LockFreeBTree bTree, Node nodeToSplit) {
            // we will need to insert the new value, it can be either in left, right, or the new median, let's figure out
            bTree.verifyInvariants(nodeToSplit);
            int leftEnd = bTree.median - 1;
            int rightStart = bTree.median + 1;
            Object medianKey = nodeToSplit.keys[bTree.median];
            Row medianValue = nodeToSplit.columns[bTree.median];
            int size = bTree.size;
            boolean withChildren = nodeToSplit.hasChildren();

            Node left = new Node(bTree.size, withChildren);
            Node right = new Node(size, withChildren);

            copyRangeToStartOfOther(nodeToSplit.keys, left.keys, 0, leftEnd + 1);
            copyRangeToStartOfOther(nodeToSplit.columns, left.columns, 0, leftEnd + 1);
            left.numberOfKeys = leftEnd + 1;

            copyRangeToStartOfOther(nodeToSplit.keys, right.keys, rightStart, size - rightStart);
            copyRangeToStartOfOther(nodeToSplit.columns, right.columns, rightStart, size - rightStart);
            right.numberOfKeys = size - rightStart;

            if (withChildren) {
                // we are in a recursive split, yeah !
                copyRangeToStartOfOther(nodeToSplit.children, left.children, 0, leftEnd + 1 + 1);
                copyRangeToStartOfOther(nodeToSplit.children, right.children, rightStart, size - (rightStart) + 1);
            }

            if (bTree.pathStack.isEmpty()) {
                // splitting root
                Node newRoot = new Node(size, true);
                newRoot.setChildrenAround(left, right, 0);
                newRoot.fastSetRow(medianKey, medianValue, 0, bTree);
                newRoot.numberOfKeys++;
                bTree.root = newRoot;
                bTree.verifyInvariants(newRoot);
            } else {
                Node parent = bTree.pathStack.pop();
                int medianSearchIndex = search(parent.keys, 0, parent.numberOfKeys(), medianKey, bTree.comparator);
                assert medianSearchIndex < 0 : "index must be negative because we are moving a new key from child to parent";
                int medianInsertIndex = -medianSearchIndex - 1;

                Node newParent = parent.fastHasKeyAt(medianInsertIndex, bTree) ? parent.copyShifted(medianInsertIndex) : parent.copy();
                newParent.fastSetRow(medianKey, medianValue, medianInsertIndex, bTree);
                newParent.setChildrenAround(left, right, medianInsertIndex);
                newParent.numberOfKeys++;
                if (bTree.pathStack.isEmpty()) {
                    bTree.root = newParent;
                    bTree.verifyInvariantsForRootNode(newParent);
                } else {
                    nodeToSplit.replaceInParent(bTree, parent, newParent);
                    bTree.verifyInvariants(newParent);
                }
                if (newParent.isFull(bTree)) {
                    bTree.verifyInvariants();
                    split(bTree, newParent);
                }
            }
            bTree.verifyInvariants();
        }

        private Node copy() {
            Node newNode = new Node(keys.length, hasChildren());
            copyWhole(keys, newNode.keys);
            copyWhole(columns, newNode.columns);
            if (hasChildren()) {
                copyWhole(children, newNode.children);
            }
            newNode.numberOfKeys = this.numberOfKeys();
            return newNode;
        }

        private Node copyShifted(int shiftFrom) {
            Node newNode = copy();
            shiftBothOneStep(newNode.keys, newNode.columns, shiftFrom);
            if (hasChildren()) {
                shiftOneStep(newNode.children, shiftFrom + 1);
            }
            return newNode;
        }

        public int numberOfKeys() {
            return numberOfKeys;
        }

        public boolean isLeaf() {
            return children == EMPTY;
        }

        public boolean hasChildren() {
            return !isLeaf();
        }

        /**
         * Should only be called by writer thread or after a freeze() and a safe publication of the changes
         */
        public boolean fastHasKeyAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return keys[index] != null;
        }

        /**
         * Should only be called by writer thread or after a freeze() and a safe publication of the changes
         */
        public boolean fastHasChildAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return children[index] != null;
        }

        public Node childAt(int index) {
            return (Node) UNSAFE.getObjectVolatile(children, byteOffset(index));
        }

        /**
         * Should only be called by writer thread or after a freeze() and a safe publication of the changes
         */
        public Node fastChildAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return children[index];
        }

        public Object fastKeyAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return keys[index];
        }

        public Object keyAt(int index) {
            return UNSAFE.getObjectVolatile(keys, byteOffset(index));
        }

        public Row valueAt(int index) {
            return (Row) UNSAFE.getObjectVolatile(columns, byteOffset(index));
        }

        public Row fastValueAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return columns[index];
        }

        public void set(Object rowKey, Map values, int index) {
            // set value first with set ordered to ensure that an other seeing the value will see it fully,
            // then set key with barrier to ensure visibility of both
            if (columns[index] == null) {
                UNSAFE.putOrderedObject(columns, byteOffset(index), new MapRow(values));
            } else {
                // row handle thread safety internally
                columns[index].addAll(values);
            }
            UNSAFE.putObjectVolatile(keys, byteOffset(index), rowKey);
        }

        /**
         * Do not enforce visibility, use only when an other op guarantee safe publication
         */
        public void fastSet(Object rowKey, Map values, int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            if (columns[index] == null) {
                columns[index] = new MapRow(values);
            } else {
                columns[index].addAll(values);
            }
            keys[index] = rowKey;
        }

        /**
         * Do not enforce visibility, use only when an other op guarantee safe publication
         */
        public void fastSetRow(Object rowKey, Row row, int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            columns[index] = row;
            keys[index] = rowKey;
        }

        private static long byteOffset(int i) {
            return ((long) i << shift) + base;
        }

        public boolean isFull(LockFreeBTree bTree) {
            return numberOfKeys == bTree.size;
        }

    }

    public void executeOnEachNode(Consumer<Node> action) {
        executeOnEachNode(action, root);
    }

    private void executeOnEachNode(Consumer<Node> action, Node node) {
        action.accept(node);
        if (node.hasChildren()) {
            for (Node child : node.children) {
                if (child == null) {
                    break;
                }
                executeOnEachNode(action, child);
            }
        }
    }

    protected void verifyInvariants() {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            executeOnEachNode(this::verifyInvariants);
        }
    }

    private void verifyInvariantsForRootNode(Node node) {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            verifyInvariants(node, true);
        }
    }

    private void verifyInvariants(Node node) {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            verifyInvariants(node, false);
        }
    }

    @VisibleForTesting
    public void forceVerifyInvariants() {
        // find duplicate objets or nodes
        IdentityHashMap<Object, Object> nodeMap = new IdentityHashMap<>();
        executeOnEachNode(node -> {
            Object put = nodeMap.put(node, "");
            if (put != null) {
                throw new IllegalStateException("duplicate for node " + node);
            }
        });

        IdentityHashMap<Object, Object> kvMap = new IdentityHashMap<>();
        applyInOrder((k, v) -> {
            Object put = kvMap.put(k, v);
            if (put != null) {
                throw new IllegalStateException("duplicate for key " + k);
            }
        });

        executeOnEachNode(this::forceVerifyInvariants);
    }

    private void forceVerifyInvariants(Node node) {
        verifyInvariants(node, false);
    }

    private void verifyInvariants(Node node, boolean skipSizeCheck) {
        Object[] keys = node.keys;
        Object[] values = node.columns;
        Node[] children = node.children;

        // no gaps
        checkCondition(() -> !hasGaps(keys), () -> "there are gaps in keys" + Arrays.toString(keys));
        checkCondition(() -> !hasGaps(values), () -> "there are gaps in columns" + Arrays.toString(values));
        checkCondition(() -> !hasGaps(children), () -> "there are gaps in children" + Arrays.toString(children));

        // correct number of entries in each array
        if (!skipSizeCheck) {
            int keysOccupancy = occupancy(keys);
            int valuesOccupancy = occupancy(values);
            int childrenOccupancy = occupancy(children);
            checkCondition(() -> keysOccupancy == valuesOccupancy, () -> "incorrect number of key and columns" + Arrays.toString(keys) + " / " + Arrays.toString(values));
            if (!node.isLeaf() && node != root) {
                checkCondition(() -> keysOccupancy + 1 == childrenOccupancy, () -> format("incorrect number of key and children %s / %s : %s / %s", keysOccupancy, childrenOccupancy, Arrays.toString(keys), Arrays.toString(children)));
            }

            // nodes except root are at least half full
            checkCondition(() -> node == root || keysOccupancy >= size / 2 - 1, () -> format("node should be at least half full (except root), size was : %s, expected at least %s : %s", keysOccupancy, size / 2, Arrays.asList(keys)));

        }

        // keys are sorted
        checkCondition(() -> isSorted(keys), () -> "keys are not sorted " + Arrays.toString(keys));

        // children are correctly ordered with keys
        if (node.hasChildren()) {
            for (int i = 0; i < keys.length - 1; i++) {
                Object key = keys[i];
                if (key == null) {
                    break;
                }
                Object lastKeyOfLeftChild = lastNonNullElement(children[i].keys);
                checkCondition(() -> comparator.compare(lastKeyOfLeftChild, key) < 0, () -> format("last key of left child %s is greater than key %s", lastKeyOfLeftChild, key));

                Object firstKeyOfRightChild = children[i + 1].keys[0];
                checkCondition(() -> comparator.compare(firstKeyOfRightChild, key) > 0, () -> format("first key of right child %s is lesser than key %s", firstKeyOfRightChild, key));
            }
        }

    }

    private void clearWriterPathStack() {
        if (ASSERTION_ENABLED) {
            while (!pathStack.isEmpty()) {
                Node node = pathStack.pop();
                assert !node.isFull(this) : "node should not be full" + node;
            }
        } else {
            pathStack.clear();
        }
    }

    private static void checkCondition(BooleanSupplier condition, Supplier<String> message) {
        if (!condition.getAsBoolean()) {
            throw new IllegalStateException(message.get());
        }
    }

    private static int search(Object[] a, int fromIndex, int toIndex, Object key, Comparator<Object> c) {
        if (USE_BINARY_SEARCH) {
            // maybe find a threshold depending on size ?
            return binarySearch(a, fromIndex, toIndex, key, c);
        }
        return linearSearch(a, fromIndex, toIndex, key, c);
    }

    private boolean isSorted(Object[] array) {
        // doesn't handle gaps
        for (int i = 0; i < array.length - 1; i++) {
            if (array[i] == null | array[i + 1] == null) {
                return true;
            }
            if (comparator.compare(array[i], array[i + 1]) >= 0) {
                return false;
            }
        }
        return true;
    }

    private void currentThreadIsWriter() {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            assert writerThread.get() == Thread.currentThread() | frozen;
        }
    }

    /**
     * Use only from writer thread or after a freeze()
     * The consumer must not modify the keys
     */
    public <K> void applyInOrder(BiConsumer<K, Row> action) {
        currentThreadIsWriter();
        Node root = this.root;
        applyInOrder(action, root);
    }

    private <K> void applyInOrder(BiConsumer<K, Row> action, Node node) {
        int numberOfKeys = node.numberOfKeys();
        for (int i = 0; i < numberOfKeys; i++) {
            if (node.hasChildren() && node.fastHasChildAt(i, this)) {
                applyInOrder(action, node.fastChildAt(i, this));
            }

            action.accept((K) node.fastKeyAt(i, this), node.fastValueAt(i, this));
        }
        int lastChildIndex = numberOfKeys;
        if (node.hasChildren() && node.fastHasChildAt(lastChildIndex, this)) {
            applyInOrder(action, node.fastChildAt(lastChildIndex, this));
        }
    }

}
