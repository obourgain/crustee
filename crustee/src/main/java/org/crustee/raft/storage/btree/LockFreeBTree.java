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
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.assertj.core.util.VisibleForTesting;
import org.crustee.raft.storage.memtable.WritableMemtable;
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
 * The structure is safe for writing by only one thread, and the method to traverse it like {@link WritableMemtable#applyInOrder(BiConsumer)}
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
public class LockFreeBTree<K, V> {

    private static final boolean AUTO_VERIFY_INVARIANTS_ENABLE = Boolean.getBoolean("btree.verify.invariants");
    private static final boolean USE_BINARY_SEARCH = false;

    private static final boolean ASSERTION_ENABLED = LockFreeBTree.class.desiredAssertionStatus();

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

    private final Comparator<K> comparator;

    private final ArrayStack<Node<K, V>> pathStack = new ArrayStack<>();
    private final int size;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final int median; // used when splitting
    private boolean frozen = false;

    // we are single writer, so volatile is ok for those
    private volatile int count = 0;
    protected volatile Node<K, V> root;

    private WeakReference<Thread> writerThread;

    public LockFreeBTree(int size, Class<K> keyClass, Class<V> valueClass) {
        this(ComparableComparator.get(), size, keyClass, valueClass);
    }

    public LockFreeBTree(Comparator<K> comparator, int size, Class<K> keyClass, Class<V> valueClass) {
        this.comparator = comparator;
        this.size = size;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.median = size / 2;
        this.root = new Node<>(this, false);
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

    public void insert(K rowKey, UpdateAction<V> updateAction) {
        assert !frozen;
        assert pathStack.isEmpty();
        currentThreadIsWriter();
        try {
            doInsert(rowKey, updateAction, root);
        } finally {
            clearWriterPathStack();
        }
    }

    private void doInsert(K rowKey, UpdateAction<V> updateAction, Node<K, V> node) {
        verifyInvariants(node);
        if (node.isLeaf()) {
            // insert in this node
            node.insert(this, rowKey, updateAction, comparator);
            return;
        }

        int i;
        for (i = 0; i < node.numberOfKeys(); i++) {
            K k = node.keyAt(i);
            int compare = comparator.compare(rowKey, k);
            if (compare < 0) {
                // lesser, we go to the left child
                doInsertInChild(rowKey, updateAction, node, i);
                return;
            }
            if (compare == 0) {
                node.set(rowKey, updateAction, i);
                return;
            }
        }

        // greater, we go to the right child or set it here if leaf
        // don't add +1 to go to the right child because the loop already incremented i
        doInsertInChild(rowKey, updateAction, node, i);
    }

    private void doInsertInChild(K rowKey, UpdateAction<V> updateAction, Node<K, V> node, int i) {
        Node<K, V> tmp = node.fastChildAt(i, this);
        assert tmp != null;
        pathStack.push(node);
        doInsert(rowKey, updateAction, tmp);
    }

    public V get(K key) {
        Node<K, V> node = root;
        return doGet(key, node);
    }

    private V doGet(K key, Node<K, V> node) {
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
            K k = node.keys[i];
            int compare = comparator.compare(key, k);
            if (compare < 0) {
                // lesser, we go to the left child
                Node<K, V> child = node.childAt(i);
                V value = doGet(key, child);
                return value;
            }
            if (compare == 0) {
                return node.valueAt(i);
            }
        }
        Node<K, V> child = node.childAt(i);
        return doGet(key, child);
    }

    protected static class Node<K, V> {
        protected final Node<K, V>[] children;
        protected final K[] keys;
        protected final V[] columns;
        private int numberOfKeys;

        private Node(LockFreeBTree<K, V> bTree, boolean withChildren) {
            children = withChildren ? new Node[bTree.size + 1] : EMPTY;
            keys = (K[]) Array.newInstance(bTree.keyClass, bTree.size);
            columns = (V[]) Array.newInstance(bTree.valueClass, bTree.size);
            numberOfKeys = 0;
        }

        /**
         * Does not ensure thread safety, this must be used only before publication of the node
         */
        private void setChildrenAround(Node<K, V> left, Node<K, V> right, int index) {
            // does NOT copy, it should only be called from a method that copies
            assert hasChildren();
            children[index] = left;
            children[index + 1] = right;
        }

        // the BTree param is not pretty but avoids one pointer per Node if it was non static inner class
        public void insert(LockFreeBTree<K, V> bTree, K rowKey, UpdateAction<V> updateAction, Comparator<K> comparator) {
            int searchIndex = search(keys, 0, numberOfKeys(), rowKey, comparator);
            Node<K, V> node = insertAt(bTree, rowKey, updateAction, comparator, searchIndex);
            bTree.verifyInvariants();
            if (node.isFull(bTree)) {
                split(bTree, node);
                bTree.verifyInvariants();
            }
        }

        // returns the new node if a copy have been created or this if changed in place
        private Node<K, V> insertAt(LockFreeBTree<K, V> bTree, K rowKey, UpdateAction<V> updateAction, Comparator<K> comparator, int searchIndex) {
            assert !isFull(bTree) : "should be split preemptively";
            int insertIndex;
            if (searchIndex < 0) { // this is the -(index + 1) where we should insert
                insertIndex = -searchIndex - 1;
                if (fastHasKeyAt(insertIndex, bTree)) {
                    // move stuff
                    Node<K, V> newNode = copyShifted(bTree, insertIndex);
                    newNode.numberOfKeys++;
                    newNode.fastSet(rowKey, updateAction, insertIndex, bTree);
                    bTree.count++;
                    if (bTree.pathStack.isEmpty()) {
                        // adding to root
                        bTree.root = newNode;
                    } else {
                        replaceInParent(bTree, this, newNode);
                    }
                    return newNode;
                } else {
                    this.set(rowKey, updateAction, insertIndex);
                    numberOfKeys++;
                    bTree.count++;
                    return this;
                }
            } else {
                // overwrite existing
                assert keys[searchIndex] != null;
                assert comparator.compare(rowKey, keys[searchIndex]) == 0;
                this.set(rowKey, updateAction, searchIndex);
                return this;
            }
        }

        private void replaceInParent(LockFreeBTree<K, V> bTree, Node<K, V> nodeToReplace, Node<K, V> newNode) {
            Node<K, V> parent = bTree.pathStack.peek();
            int indexOfInstance = findIndexOfInstance(parent.children, nodeToReplace);
            assert indexOfInstance >= 0;
            UNSAFE.putObjectVolatile(parent.children, byteOffset(indexOfInstance), newNode);
        }

        private static <K, V> void split(LockFreeBTree<K, V> bTree, Node<K, V> nodeToSplit) {
            // we will need to insert the new value, it can be either in left, right, or the new median, let's figure out
            bTree.verifyInvariants(nodeToSplit);
            int leftEnd = bTree.median - 1;
            int rightStart = bTree.median + 1;
            K medianKey = nodeToSplit.keys[bTree.median];
            V medianValue = nodeToSplit.columns[bTree.median];
            boolean withChildren = nodeToSplit.hasChildren();
            int size = bTree.size;

            Node<K, V> left = new Node<>(bTree, withChildren);
            Node<K, V> right = new Node<>(bTree, withChildren);

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
                Node<K, V> newRoot = new Node<>(bTree, true);
                newRoot.setChildrenAround(left, right, 0);
                newRoot.fastReplaceValue(medianKey, medianValue, 0, bTree);
                newRoot.numberOfKeys++;
                bTree.root = newRoot;
                bTree.verifyInvariants(newRoot);
            } else {
                Node<K, V> parent = bTree.pathStack.pop();
                int medianSearchIndex = search(parent.keys, 0, parent.numberOfKeys(), medianKey, bTree.comparator);
                assert medianSearchIndex < 0 : "index must be negative because we are moving a new key from child to parent";
                int medianInsertIndex = -medianSearchIndex - 1;

                Node<K, V> newParent = parent.fastHasKeyAt(medianInsertIndex, bTree) ? parent.copyShifted(bTree, medianInsertIndex) : parent.copy(bTree);
                newParent.fastReplaceValue(medianKey, medianValue, medianInsertIndex, bTree);
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

        private Node<K, V> copy(LockFreeBTree<K, V> bTree) {
            Node<K, V> newNode = new Node<>(bTree, hasChildren());
            copyWhole(keys, newNode.keys);
            copyWhole(columns, newNode.columns);
            if (hasChildren()) {
                copyWhole(children, newNode.children);
            }
            newNode.numberOfKeys = this.numberOfKeys();
            return newNode;
        }

        private Node<K, V> copyShifted(LockFreeBTree<K, V> bTree, int shiftFrom) {
            Node<K, V> newNode = copy(bTree);
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

        public Node<K, V> childAt(int index) {
            return (Node<K, V>) UNSAFE.getObjectVolatile(children, byteOffset(index));
        }

        /**
         * Should only be called by writer thread or after a freeze() and a safe publication of the changes
         */
        public Node<K, V> fastChildAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return children[index];
        }

        public K fastKeyAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return keys[index];
        }

        public K keyAt(int index) {
            return (K) UNSAFE.getObjectVolatile(keys, byteOffset(index));
        }

        public V valueAt(int index) {
            return (V) UNSAFE.getObjectVolatile(columns, byteOffset(index));
        }

        public V fastValueAt(int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            return columns[index];
        }

        public void set(K rowKey, UpdateAction<V> updateAction, int index) {
            // set value first with set ordered to ensure that an other seeing the value will see it fully,
            // then set key with barrier to ensure visibility of both
            if (columns[index] == null) {
                UNSAFE.putOrderedObject(columns, byteOffset(index), updateAction.insert());
            } else {
                // row handle thread safety internally
                updateAction.merge(columns[index]);
            }
            UNSAFE.putObjectVolatile(keys, byteOffset(index), rowKey);
        }

        /**
         * Do not enforce visibility, use only when an other op guarantee safe publication
         */
        public void fastSet(K rowKey, UpdateAction<V> updateAction, int index, LockFreeBTree bTree) {
            bTree.currentThreadIsWriter();
            if (columns[index] == null) {
                columns[index] = updateAction.insert();
            } else {
                updateAction.merge(columns[index]);
            }
            keys[index] = rowKey;
        }

        /**
         * Do not enforce visibility, use only when an other op guarantee safe publication
         * Used for splitting rows.
         */
        private void fastReplaceValue(K rowKey, V value, int index, LockFreeBTree<K, V> bTree) {
            bTree.currentThreadIsWriter();
            columns[index] = value;
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

    private void verifyInvariantsForRootNode(Node<K, V> node) {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            verifyInvariants(node, true);
        }
    }

    private void verifyInvariants(Node<K, V> node) {
        if (AUTO_VERIFY_INVARIANTS_ENABLE) {
            verifyInvariants(node, false);
        }
    }

    @VisibleForTesting
    public void forceVerifyInvariants() {
        // find duplicate objects or nodes
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

    private void forceVerifyInvariants(Node<K, V> node) {
        verifyInvariants(node, false);
    }

    private void verifyInvariants(Node<K, V> node, boolean skipSizeCheck) {
        K[] keys = node.keys;
        V[] values = node.columns;
        Node<K, V>[] children = node.children;

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
                K key = keys[i];
                if (key == null) {
                    break;
                }
                K lastKeyOfLeftChild = lastNonNullElement(children[i].keys);
                checkCondition(() -> comparator.compare(lastKeyOfLeftChild, key) < 0, () -> format("last key of left child %s is greater than key %s", lastKeyOfLeftChild, key));

                K firstKeyOfRightChild = children[i + 1].keys[0];
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

    private static <K> int search(K[] a, int fromIndex, int toIndex, K key, Comparator<K> c) {
        if (USE_BINARY_SEARCH) {
            // maybe find a threshold depending on size ?
            return binarySearch(a, fromIndex, toIndex, key, c);
        }
        return linearSearch(a, fromIndex, toIndex, key, c);
    }

    private boolean isSorted(K[] array) {
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
    public void applyInOrder(BiConsumer<K, V> action) {
        currentThreadIsWriter();
        Node<K, V> root = this.root;
        applyInOrder(action, root);
    }

    private void applyInOrder(BiConsumer<K, V> action, Node<K, V> node) {
        int numberOfKeys = node.numberOfKeys();
        for (int i = 0; i < numberOfKeys; i++) {
            if (node.hasChildren() && node.fastHasChildAt(i, this)) {
                applyInOrder(action, node.fastChildAt(i, this));
            }

            action.accept(node.fastKeyAt(i, this), node.fastValueAt(i, this));
        }
        int lastChildIndex = numberOfKeys;
        if (node.hasChildren() && node.fastHasChildAt(lastChildIndex, this)) {
            applyInOrder(action, node.fastChildAt(lastChildIndex, this));
        }
    }

    public interface UpdateAction<V> {
        V merge(V current);

        V insert();
    }
}
