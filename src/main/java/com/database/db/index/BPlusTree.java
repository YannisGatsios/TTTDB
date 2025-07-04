package com.database.db.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import java.util.LinkedList;
import java.util.Queue;

/**
 * B+ Tree implementation supporting configurable order, unique/non-unique keys,
 * and efficient CRUD operations. Maintains sorted key-value pairs with logarithmic
 * time complexity for core operations.
 *
 * <p>Key Features:
 * <ul>
 *   <li>Automatic height adjustment during insertions/deletions</li>
 *   <li>Leaf node chaining for efficient range scans</li>
 *   <li>Support for duplicate keys (when uniqueness is disabled)</li>
 *   <li>Dynamic node splitting/merging with borrowing optimizations</li>
 * </ul>
 *
 * @param <K> Key type (must implement Comparable)
 * @param <V> Value type associated with keys
 */
public class BPlusTree<K extends Comparable<? super K>, V> implements BTree<K, V> {

    private Node<K, V> root;
    private Node<K, V> start;// Points to the first leaf node
    private final int order;
    private int size = 0;
    private boolean isUnique;
    private final int minKeys;
    private final Comparator<Pair<K, V>> keyComparator = (pair1, pair2) -> pair1.key.compareTo(pair2.key);

    /**
     * Constructs a B+ Tree with specified order.
     *
     * @param order Tree order (minimum 3). Determines max keys per node (order-1).
     * @throws IllegalArgumentException if order < 3
     */
    public BPlusTree(int order) {
        if (order < 3)
            throw new IllegalArgumentException("B+Tree Order must be at least 3.");
        this.root = new Node<>(true);// Start with an empty leaf root
        this.start = root;
        this.order = order;
        this.size = 0;
        this.isUnique = true;
        this.minKeys = (int) Math.ceil((double) this.order / 2.0) - 1;
    }

    // ========! INSERTION !==========
    // Core Operation (see interface docs for details)
    public void insert(K key, V value) {
        Pair<K, V> newPair = new Pair<>(key, value);
        // Split root if it's full
        if (this.root.pairs.size() == order - 1) {
            splitRoot();
        }
        insertNonFull(this.root, newPair);
    }

    private void splitRoot() {
        Node<K, V> newRoot = new Node<>(false);
        Node<K, V> oldRoot = root;
        root = newRoot;
        newRoot.children.add(oldRoot);
        oldRoot.parent = newRoot;
        splitChild(newRoot, 0);
    }

    private void splitChild(Node<K, V> parentNode, int childIndex) {
        Node<K, V> leftChild = parentNode.children.get(childIndex);
        Node<K, V> newRightChild = new Node<>(leftChild.isLeaf);
        newRightChild.parent = parentNode;
        if (leftChild.isLeaf) {
            splitLeaf(leftChild, newRightChild, childIndex);
        } else {
            splitInternal(leftChild, newRightChild, childIndex);
        }
        // Recursive parent splitting added here
        if (parentNode.pairs.size() >= order) {
            if (parentNode == root) {
                splitRoot();
            } else {
                int indexInGrandparent = parentNode.parent.children.indexOf(parentNode);
                splitChild(parentNode.parent, indexInGrandparent);
            }
        }
    }

    private void splitLeaf(Node<K, V> leftChild, Node<K, V> rightChild, int childIndex) {
        int splitPoint = leftChild.pairs.size() / 2;
        // Move pairs to new node
        rightChild.pairs.addAll(leftChild.pairs.subList(splitPoint, leftChild.pairs.size()));
        leftChild.pairs.subList(splitPoint, leftChild.pairs.size()).clear();
        // Update leaf links
        rightChild.next = leftChild.next;
        leftChild.next = rightChild;
        // Promote first key of right child
        K promotedKey = rightChild.pairs.getFirst().key;
        Node<K, V> parent = leftChild.parent;
        // Insert in parent at correct position
        parent.pairs.add(childIndex, new Pair<>(promotedKey, null));
        parent.children.add(childIndex + 1, rightChild);
    }

    private void splitInternal(Node<K, V> leftChild, Node<K, V> rightChild, int childIndex) {
        int midIndex = leftChild.pairs.size() / 2;
        Pair<K, V> promotedPair = leftChild.pairs.get(midIndex);
        leftChild.pairs.remove(midIndex);
        // Move keys and children to the new node
        rightChild.pairs.addAll(leftChild.pairs.subList(midIndex, leftChild.pairs.size()));
        rightChild.children.addAll(leftChild.children.subList(midIndex + 1, leftChild.children.size()));
        // Clean up the child node
        leftChild.pairs.subList(midIndex, leftChild.pairs.size()).clear();
        leftChild.children.subList(midIndex + 1, leftChild.children.size()).clear();
        // Update parent pointers for the new node's children
        for (Node<K, V> child : rightChild.children) {
            child.parent = rightChild;
        }
        // Insert promoted key and new child into the parent (immediate parent)
        rightChild.parent.pairs.add(childIndex, promotedPair);
        rightChild.parent.children.add(childIndex + 1, rightChild);
    }

    private void insertNonFull(Node<K, V> node, Pair<K, V> pair) {
        if (node.isLeaf) {
            insertIntoLeaf(node, pair);
        } else {
            insertIntoInternal(node, pair);
        }
    }

    private void insertIntoLeaf(Node<K, V> leaf, Pair<K, V> pair) {
        int pos = Collections.binarySearch(leaf.pairs, pair, keyComparator);
        if (pos >= 0 && isUnique) {
            return; // Key exists and tree is unique
        } else if (pos >= 0) {
            // Handle duplicates by adding to the existing pair's duplicates
            if (!leaf.pairs.get(pos).value.equals(pair.value)) leaf.pairs.get(pos).addDup(pair.value);
            this.size++;
        } else {
            pos = -(pos + 1);
            leaf.pairs.add(pos, pair);
            this.size++;
            if (leaf.pairs.size() >= order) {
                if (leaf == root) {
                    splitRoot(); // Special handling for root leaf
                } else {
                    splitChild(leaf.parent, parentChildIndex(leaf));
                }
            }
        }
    }

    private int parentChildIndex(Node<K, V> child) {
        return child.parent.children.indexOf(child);
    }

    private void insertIntoInternal(Node<K, V> node, Pair<K, V> pair) {
        int index = findChildIndex(node, pair.key);
        Node<K, V> child = node.children.get(index);
        if (child.pairs.size() >= order) {
            splitChild(node, index);
            // Adjust index after split
            if (pair.key.compareTo(node.pairs.get(index).key) > 0) {
                index++;
            }
        }
        insertNonFull(node.children.get(index), pair);
    }

    private int findChildIndex(Node<K, V> node, K key) {
        int index = 0;
        while (index < node.pairs.size() && key.compareTo(node.pairs.get(index).key) >= 0) {
            index++;
        }
        return index;
    }

    // ===========! REMOVING !=============
    // Core Operation (see interface docs for details)
    public void remove(K key, V value) {
        if (key == null || root == null) return;
        // 1. Find the leaf node where the key should exist.
        Node<K, V> leaf = findLeafNode(key);
        if (leaf == null) return; // Key is not in the tree.
        // 2. Remove the entry from the leaf node.
        boolean wasRemoved = removeFromLeaf(leaf, key, value);
        // If the specified key-value pair was not found and removed, we are done.
        if (!wasRemoved) return;
        // 3. If the root is a leaf with no more entries, the tree is now empty.
        if (root.isLeaf && root.pairs.isEmpty()) {
            this.start = root; // 'start' points to the now-empty root/leaf.
            return;
        }
        // 4. Handle underflow. If the leaf has too few keys, we must rebalance.
        // We use a bottom-up approach, starting from the modified leaf.
        Node<K, V> currentNode = leaf;
        while (currentNode != root && currentNode.pairs.size() < minKeys) {
            Node<K, V> parent = currentNode.parent;
            int childIndex = parent.children.indexOf(currentNode);
            handleUnderflow(parent, currentNode, childIndex);
            currentNode = parent; // Move up to the parent to check for cascading underflow.
        }
        // 5. If the root is an internal node that has lost all its keys,
        // its only remaining child becomes the new root, shrinking the tree's height.
        if (!root.isLeaf && root.pairs.isEmpty()) {
            root = root.children.getFirst();
            root.parent = null;
        }
        if (!root.isLeaf) {
            for (int i = 0; i < root.pairs.size(); i++) {
                Node<K, V> child = root.children.get(i + 1);
                K firstKey = child.getFirstLeafKey();
                if (firstKey != null) {
                    root.pairs.get(i).key = firstKey;
                }
            }
        }
    }

    private Node<K, V> findLeafNode(K key) {
        Node<K, V> current = this.root;
        while (!current.isLeaf) {
            current = current.children.get(findChildIndex(current, key));
        }
        return current;
    }

    private boolean removeFromLeaf(Node<K, V> node, K key, V value) {
        Pair<K, V> temp = new Pair<>(key, value);
        int index = Collections.binarySearch(node.pairs, temp, keyComparator);
        if (index < 0) return false; // Key does not exist.
        boolean keyWasFirst = (index == 0);
        // Handle unique vs. non-unique cases
        if (isUnique) {
            if(node.pairs.get(index).value.equals(value)) {
                node.pairs.remove(index);
                this.size--;
            }
        } else {
            Pair<K, V> pair = node.pairs.get(index);
            if (pair.value.equals(value)) {
                // The primary value matches. Promote a duplicate if one exists.
                if (pair.getDuplicates() != null && !pair.getDuplicates().isEmpty()) {
                    pair.value = pair.getDuplicates().iterator().next();
                    pair.removeDup(pair.value);
                    this.size--;
                } else {
                    // This was the last value for this key. Remove the pair.
                    node.pairs.remove(index);
                    this.size--;
                }
            } else if (pair.getDuplicates() != null && pair.getDuplicates().contains(value)) {
                // The value to remove is in the duplicate set.
                pair.removeDup(value);
                this.size--;
            } else {
                return false; // The specific value was not found for this key.
            }
        }

        // CRITICAL STEP: If we removed the first key in the leaf, the separator key
        // in one of the parent nodes might now be incorrect. We must update it.
        if (keyWasFirst && node.parent != null && !node.pairs.isEmpty()) {
            int childIndex = parentChildIndex(node);
            if (childIndex > 0) {
                node.parent.pairs.get(childIndex - 1).key = node.pairs.getFirst().key;
            }
        }
        return true;
    }

    private void handleUnderflow(Node<K, V> parent, Node<K, V> child, int childIndex) {
        // Try borrowing from the left sibling first.
        if (childIndex > 0) {
            Node<K, V> leftSibling = parent.children.get(childIndex - 1);
            if (leftSibling.pairs.size() > minKeys) {
                borrowFromLeft(parent, child, leftSibling, childIndex);
                return;
            }
        }
        // Try borrowing from the right sibling.
        if (childIndex < parent.children.size() - 1) {
            Node<K, V> rightSibling = parent.children.get(childIndex + 1);
            if (rightSibling.pairs.size() > minKeys) {
                borrowFromRight(parent, child, rightSibling, childIndex);
                return;
            }
        }
        // If borrowing is not possible, we must merge.
        // Merge with the right sibling if it exists, otherwise with the left.
        if (childIndex < parent.children.size() - 1) {
            mergeNodes(parent, childIndex, childIndex + 1);
        } else if (childIndex > 0) {
            mergeNodes(parent, childIndex - 1, childIndex);
        }
    }

    private void borrowFromLeft(Node<K, V> parent, Node<K, V> child, Node<K, V> leftSibling, int childIndex) {
        if (leftSibling.pairs.isEmpty())
            return;
        if (child.isLeaf) {
            // Leaf node rotation (right rotation)
            Pair<K, V> borrowedPair = leftSibling.pairs.removeLast();
            child.pairs.addFirst(borrowedPair);
            parent.pairs.set(childIndex - 1, new Pair<>(borrowedPair.key, null));
        } else {
            // Internal node rotation
            Pair<K, V> borrowedPair = leftSibling.pairs.removeLast();
            Node<K, V> borrowedChild = leftSibling.children.removeLast();
            borrowedChild.parent = child;
            Pair<K, V> separator = parent.pairs.get(childIndex - 1);
            child.pairs.addFirst(separator);
            child.children.addFirst(borrowedChild);
            parent.pairs.set(childIndex - 1, borrowedPair);
        }
    }

    private void borrowFromRight(Node<K, V> parent, Node<K, V> child, Node<K, V> rightSibling, int childIndex) {
        if (rightSibling.pairs.isEmpty())
            return;
        if (child.isLeaf) {
            // Leaf node rotation (left rotation)
            Pair<K, V> borrowedPair = rightSibling.pairs.removeFirst();
            child.pairs.addLast(borrowedPair);
            parent.pairs.set(childIndex, new Pair<>(rightSibling.pairs.getFirst().key, null));
        } else {
            // Internal node rotation
            Pair<K, V> borrowedPair = rightSibling.pairs.removeFirst();
            Node<K, V> borrowedChild = rightSibling.children.removeFirst();
            borrowedChild.parent = child;
            Pair<K, V> separator = parent.pairs.get(childIndex);
            child.pairs.addLast(separator);
            child.children.addLast(borrowedChild);
            parent.pairs.set(childIndex, borrowedPair);
        }
    }

    private void mergeNodes(Node<K, V> parent, int leftIndex, int rightIndex) {
        Node<K, V> leftChild = parent.children.get(leftIndex);
        Node<K, V> rightChild = parent.children.get(rightIndex);
        // If the nodes are internal, we must also pull down the separator key from the
        // parent.
        if (!leftChild.isLeaf) {
            Pair<K, V> separator = parent.pairs.get(leftIndex);
            leftChild.pairs.add(separator);
        }
        // Move all pairs and children from the right node to the left node.
        leftChild.pairs.addAll(rightChild.pairs);
        if (!leftChild.isLeaf) {
            for (Node<K, V> node : rightChild.children) {
                node.parent = leftChild;
            }
            leftChild.children.addAll(rightChild.children);
        }
        // If they are leaves, update the linked list pointer.
        if (leftChild.isLeaf) {
            leftChild.next = rightChild.next;
            if (!leftChild.pairs.isEmpty()) {
                int newLeftIndex = parent.children.indexOf(leftChild);
                if (newLeftIndex > 0) {
                    parent.pairs.get(newLeftIndex - 1).key = leftChild.pairs.getFirst().key;
                }
            }
        }
        // Remove the separator and the pointer to the right child from the parent,
        // as the right child is now empty and obsolete.
        parent.pairs.remove(leftIndex);
        parent.children.remove(rightIndex);
    }

    // ==========! SEARCHING !===========
    // Core Operation (see interface docs for details)
    public Pair<K, V> search(K key) {
        Node<K, V> node = this.findNode(key);
        if (node == null)
            return null;
        int idx = Collections.binarySearch(node.pairs, new Pair<>(key, null), keyComparator);
        if (idx >= 0) {
            return node.pairs.get(idx);
        }
        return null;
    }
    // Core Operation (see interface docs for details)
    public boolean isKey(K key) {
        if (this.root == null)
            return false;
        return this.search(key) != null;
    }

    private Node<K, V> findNode(K key) {
        Node<K, V> current = root;
        while (current != null && !current.isLeaf) {
            int index = 0;
            // Find the correct child index
            while (index < current.pairs.size() && key.compareTo(current.pairs.get(index).key) >= 0) {
                index++;
            }
            current = current.children.get(index);
        }
        return current;
    }
    // Core Operation (see interface docs for details)
    public List<Pair<K, V>> rangeSearch(K fromKey, K toKey) {
        Node<K, V> current;
        int idx = 0;
        if (fromKey != null) {
            current = findNode(fromKey);
            if (current == null)
                return new ArrayList<>();
            for (idx = 0; idx < current.pairs.size(); idx++) {
                if (current.pairs.get(idx).key.compareTo(fromKey) >= 0) {
                    break;
                }
            }
        } else {
            current = start;
        }
        List<Pair<K, V>> result = new ArrayList<>();
        while (current != null) {
            int i = idx;
            while (i < current.pairs.size() && (toKey == null || current.pairs.get(i).key.compareTo(toKey) <= 0)) {
                result.add(current.pairs.get(i));
                i++;
            }
            if (i == current.pairs.size() && (toKey == null
                    || (!current.pairs.isEmpty() && current.pairs.getLast().key.compareTo(toKey) <= 0))) {
                current = current.next;
                idx = 0;
            } else {
                break;
            }
        }
        return result;
    }

    // ===============UPDATE=============
    /**
     * Updates a value (assumes unique keys).
     *
     * @param key Key to update
     * @param newValue New value to set
     */
    public void update(K key, V newValue) throws IllegalStateException{
        if (!this.isUnique) throw new IllegalStateException("Must specify oldValue in non-unique trees.");
        this.update(key, newValue, null);
    }

    /**
     * Updates a value associated with a key. For non-unique trees,
     * specifies old value to update.
     *
     * @param key Key to update
     * @param newValue New value to set
     * @param oldValue Old value to replace (required for non-unique trees)
     */
    public void update(K key, V newValue, V oldValue) throws IllegalStateException {
        if (this.isUnique && oldValue != null) throw new IllegalStateException("Cannot specify oldValue in unique trees.");
        if (key == null || newValue == null) return;
        Node<K, V> node = this.findNode(key);
        int idx = Collections.binarySearch(node.pairs, new Pair<>(key, null), keyComparator);
        if (isUnique) {
            if (idx < 0)
                return;
            node.pairs.get(idx).value = newValue;
        } else {
            // For non-unique trees, update in place
            if (idx < 0) return;
            if (node.pairs.get(idx).value.equals(oldValue)) node.pairs.get(idx).value = newValue;
            if (node.pairs.get(idx).getDuplicates()!=null && node.pairs.get(idx).getDuplicates().contains(oldValue)) {
                node.pairs.get(idx).removeDup(oldValue);
                node.pairs.get(idx).addDup(newValue);
            }
        }
    }

    // =======! PRINTING !======
    /**
     * @return String representation of tree levels and leaf sequence
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (root == null || root.pairs.isEmpty()) {
            return "Tree is empty.";
        }
        Queue<Node<K, V>> queue = new LinkedList<>();
        queue.add(root);
        int level = 0;
        while (!queue.isEmpty()) {
            int levelSize = queue.size();
            sb.append("!=========! Level ").append(level).append(" (").append(levelSize)
                    .append(" Nodes) !=========!\n");
            for (int i = 0; i < levelSize; i++) {
                Node<K, V> current = queue.poll();
                sb.append(current.toString());
                if (i < levelSize - 1)
                    sb.append("  --  "); // Separator
                if (!current.isLeaf) {
                    queue.addAll(current.children);
                }
            }
            sb.append("\n\n");
            level++;
        }
        sb.append("!=========! Leaf Chain !=========!\n");
        Node<K, V> leaf = this.start;
        while (leaf != null) {
            sb.append(leaf);
            if (leaf.next != null)
                sb.append(" -> ");
            leaf = leaf.next;
        }
        sb.append("\n");
        return sb.toString();
    }

    // Configuration & Accessors
    /**
     * Enables/disables key uniqueness.
     *
     * @param unique true to enforce unique keys, false to allow duplicates
     */
    public void setUnique(boolean unique) {this.isUnique = unique;}
    /** @return Root node of tree */
    public Node<K, V> getRoot() {return this.root;}
    /** @return First leaf node in sequence */
    public Node<K, V> getFirst() {return this.start;}
    /** @return Number of keys(if unique) or values(if not unique) in the tree */
    public int size(){return this.size;}
    /** @return Order of the tree */
    public int getOrder() {return this.order;}
}