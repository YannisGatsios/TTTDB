package com.database.tttdb.core.index.redBlackTreeIndex;
import java.util.*;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.Pair;

/**
 * Red-Black Tree based index.
 * - Unique mode: one value per key.
 * - Non-unique mode: multiple values per key (deduplicated).
 * - Nullable keys supported via a side bucket.
 */
public class RedBlackTreeIndex<K extends Comparable<? super K>, V> implements Index<K, V> {

    // ---- configuration ----
    private boolean unique = false;
    private boolean nullable = false;

    // ---- accounting ----
    private long size = 0;

    // ---- null-key bucket ----
    private Pair<K, V> nullPair = null;

    // ---- RBT internals ----
    private static final boolean RED = true, BLACK = false;

    private final class Node {
        K key;
        Pair<K, V> pair;         // stores primary + duplicates
        Node left, right, parent;
        boolean color = RED;
        Node(K key, V value) {
            this.key = key;
            this.pair = new Pair<>(key, value);
        }
    }

    private Node root;

    // ===== Index<K,V> API =====

    public void insert(K key, V value) {
        if (key == null) {
            ensureNullable();
            if (nullPair == null) {
                nullPair = new Pair<>(null, value);
                size++;
            } else {
                if (unique) throw new IllegalArgumentException("Duplicate null key not allowed");
                if (!containsValue(nullPair, value)) {
                    addDuplicate(nullPair, value);
                    size++;
                }
            }
            return;
        }

        Node current = root, node = null;
        int cmp = 0;
        while (current != null) {
            node = current;
            cmp = key.compareTo(current.key);
            if (cmp == 0) {
                // key exists
                if (unique) {
                    throw new IllegalArgumentException("Duplicate key not allowed: " + key);
                } else {
                    if (!containsValue(current.pair, value)) {
                        addDuplicate(current.pair, value);
                        size++;
                    }
                }
                return;
            }
            current = (cmp < 0) ? current.left : current.right;
        }
        // new key
        Node newNode = new Node(key, value);
        newNode.parent = node;
        if (node == null) root = newNode;
        else if (cmp < 0) node.left = newNode;
        else node.right = newNode;
        insertFixup(newNode);
        size++;
    }

    public void remove(K key, V value) {
        if (key == null) {
            if (nullPair == null) return;
            if (removeFromPair(nullPair, value)) {
                size--;
                if (isPairEmpty(nullPair)) nullPair = null;
            }
            return;
        }

        Node x = findNode(key);
        if (x == null) return;

        if (equalsValue(x.pair.value, value)) {
            // removing primary value
            if (x.pair.getDuplicates() == null || x.pair.getDuplicates().isEmpty()) {
                // remove whole node from RBT
                deleteNode(x);
            } else {
                // promote one duplicate to primary
                Iterator<V> it = x.pair.getDuplicates().iterator();
                V promoted = it.next();
                it.remove();
                x.pair.value = promoted;
            }
            size--;
        } else if (containsInDuplicates(x.pair, value)) {
            x.pair.removeDup(value);
            size--;
        }
    }

    public List<Pair<K, V>> search(K key) {
        if (key == null) {
            if (nullPair == null) return List.of();
            return nullPair.getAllPairs();
        }
        Node n = findNode(key);
        return (n == null) ? List.of() : n.pair.getAllPairs();
    }

    public List<Pair<K, V>> rangeSearch(K fromKey, K toKey) {
        List<Pair<K, V>> out = new ArrayList<>();
        // null key handling: treat null as -infinity for range semantics
        if (nullable && nullPair != null) {
            boolean includeNull =
                (fromKey == null) || // unbounded lower includes null
                // if bounded lower, null < fromKey so excluded
                false;
            if (includeNull) out.addAll(nullPair.getAllPairs());
        }
        inOrderWithBounds(root, fromKey, toKey, out);
        return out;
    }

    public boolean isKey(K key) {
        if (key == null) return nullPair != null;
        return findNode(key) != null;
    }

    public void update(K key, V newValue) {
        if (!unique) throw new UnsupportedOperationException("update(key,newValue) requires unique index");
        if (key == null) {
            ensureNullable();
            if (nullPair == null) throw new NoSuchElementException("Key not found: null");
            nullPair.value = newValue;
            return;
        }
        Node node = findNode(key);
        if (node == null) throw new NoSuchElementException("Key not found: " + key);
        node.pair.value = newValue;
    }

    public void update(K key, V newValue, V oldValue) {
        if (unique) throw new UnsupportedOperationException("update(key,new,old) requires non-unique index");
        if (key == null) {
            ensureNullable();
            if (nullPair == null) throw new NoSuchElementException("Key not found: null");
            // replace in null bucket
            if (equalsValue(nullPair.value, oldValue)) {
                if (!equalsValue(oldValue, newValue)) {
                    if (containsValue(nullPair, newValue)) {
                        // new already exists -> drop old, net -1
                        if (removeFromPair(nullPair, oldValue)) size--;
                    } else {
                        nullPair.value = newValue;
                    }
                }
                return;
            }
            if (containsInDuplicates(nullPair, oldValue)) {
                nullPair.removeDup(oldValue);
                if (!containsValue(nullPair, newValue)) addDuplicate(nullPair, newValue);
                else size--; // replaced with existing -> net -1
                return;
            }
            throw new NoSuchElementException("Value not found under null key");
        }

        Node node = findNode(key);
        if (node == null) throw new NoSuchElementException("Key not found: " + key);

        if (equalsValue(node.pair.value, oldValue)) {
            if (!equalsValue(oldValue, newValue)) {
                if (containsValue(node.pair, newValue)) {
                    // drop old, already had new
                    size--;
                } else {
                    node.pair.value = newValue;
                }
            }
            return;
        }
        if (containsInDuplicates(node.pair, oldValue)) {
            node.pair.removeDup(oldValue);
            if (!containsValue(node.pair, newValue)) addDuplicate(node.pair, newValue);
            else size--;
            return;
        }
        throw new NoSuchElementException("Value not found for key: " + key);
    }

    public long size() {
        return size;
    }

    public K getMax() {
        if (root == null) return (nullable && nullPair != null) ? null : null;
        Node x = root;
        while (x.right != null) x = x.right;
        return x.key;
    }

    public void setUnique(boolean isUnique) {
        if (this.unique == isUnique) return;
        // basic safety: switching modes only allowed on empty structure
        if (size != 0)
            throw new IllegalStateException("Change uniqueness only when empty");
        this.unique = isUnique;
    }

    public void setNullable(boolean isNullable) {
        if (this.nullable == isNullable) return;
        if (!isNullable && nullPair != null)
            throw new IllegalStateException("Cannot disable nullable with existing null key");
        this.nullable = isNullable;
    }

    public boolean isUnique() { return unique; }

    public boolean isNullable() { return nullable; }

    public void clear(){
        root = null;
        nullPair = null;
        size = 0;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (nullable && nullPair != null) {
            for (Pair<K,V> p : nullPair.getAllPairs()) sb.append(p).append('\n');
        }
        inOrderDump(root, sb);
        return sb.toString();
    }

    // ===== helpers =====

    private void ensureNullable() {
        if (!nullable) throw new IllegalArgumentException("Null keys not allowed");
    }

    private Node findNode(K key) {
        Node x = root;
        while (x != null) {
            int c = key.compareTo(x.key);
            if (c == 0) return x;
            x = (c < 0) ? x.left : x.right;
        }
        return null;
    }

    private static <V> boolean equalsValue(V a, V b) {
        return Objects.equals(a, b);
    }

    private static <K,V> boolean containsInDuplicates(Pair<K,V> p, V v) {
        return p.getDuplicates() != null && p.getDuplicates().contains(v);
    }

    private static <K,V> boolean containsValue(Pair<K,V> p, V v) {
        if (Objects.equals(p.value, v)) return true;
        return containsInDuplicates(p, v);
    }

    private static <K,V> void addDuplicate(Pair<K,V> p, V v) {
        p.addDup(v);
    }

    private static <K,V> boolean removeFromPair(Pair<K,V> p, V v) {
        if (Objects.equals(p.value, v)) {
            if (p.getDuplicates() == null || p.getDuplicates().isEmpty()) {
                // caller will drop the container
                return true;
            } else {
                // promote one duplicate
                Iterator<V> it = p.getDuplicates().iterator();
                V promoted = it.next();
                it.remove();
                p.value = promoted;
                return true;
            }
        } else if (containsInDuplicates(p, v)) {
            p.removeDup(v);
            return true;
        }
        return false;
    }

    private static <K,V> boolean isPairEmpty(Pair<K,V> p) {
        return p.getDuplicates() == null || p.getDuplicates().isEmpty();
    }

    private void inOrderDump(Node x, StringBuilder sb) {
        if (x == null) return;
        inOrderDump(x.left, sb);
        for (Pair<K,V> p : x.pair.getAllPairs()) sb.append(p).append('\n');
        inOrderDump(x.right, sb);
    }

    private void inOrderWithBounds(Node x, K lo, K hi, List<Pair<K,V>> out) {
        if (x == null) return;
        boolean goLeft = lo == null || x.key.compareTo(lo) >= 0;
        boolean goRight = hi == null || x.key.compareTo(hi) <= 0;
        if (goLeft) inOrderWithBounds(x.left, lo, hi, out);
        if ((lo == null || x.key.compareTo(lo) >= 0) && (hi == null || x.key.compareTo(hi) <= 0)) {
            out.addAll(x.pair.getAllPairs());
        }
        if (goRight) inOrderWithBounds(x.right, lo, hi, out);
    }

    // ===== RBT algorithms =====

    private void leftRotate(Node node) {
        Node parent = node.right;
        node.right = parent.left;
        if (parent.left != null) parent.left.parent = node;
        parent.parent = node.parent;
        if (node.parent == null) root = parent;
        else if (node == node.parent.left) node.parent.left = parent;
        else node.parent.right = parent;
        parent.left = node;
        node.parent = parent;
    }

    private void rightRotate(Node node) {
        Node parent = node.left;
        node.left = parent.right;
        if (parent.right != null) parent.right.parent = node;
        parent.parent = node.parent;
        if (node.parent == null) root = parent;
        else if (node == node.parent.left) node.parent.left = parent;
        else node.parent.right = parent;
        parent.right = node;
        node.parent = parent;
    }

    private void insertFixup(Node node) {
        while (node.parent != null && node.parent.color == RED) {
            Node uncle;
            if (node.parent == node.parent.parent.left) {
                uncle = node.parent.parent.right;
                rightUncleInsertFixup(node, uncle);
            } else {
                uncle = node.parent.parent.left;
                leftUncleInsertFixup(node, uncle);
            }
        }
        root.color = BLACK;
    }
    private void rightUncleInsertFixup(Node node,Node rightUncle){
        if (colorOf(rightUncle) == RED) {
            node.parent.color = BLACK;
            rightUncle.color = BLACK;
            node.parent.parent.color = RED;
            node = node.parent.parent;
        } else {
            if (node == node.parent.right) {
                node = node.parent;
                leftRotate(node);
            }
            node.parent.color = BLACK;
            node.parent.parent.color = RED;
            rightRotate(node.parent.parent);
        }
    }
    private void leftUncleInsertFixup(Node node,Node leftUncle){
        if (colorOf(leftUncle) == RED) {
            node.parent.color = BLACK;
            leftUncle.color = BLACK;
            node.parent.parent.color = RED;
            node = node.parent.parent;
        } else {
            if (node == node.parent.left) {
                node = node.parent;
                rightRotate(node);
            }
            node.parent.color = BLACK;
            node.parent.parent.color = RED;
            leftRotate(node.parent.parent);
        }
    }

    private void deleteNode(Node z) {
        Node y = z, x, xParent;
        boolean yOriginalColor = y.color;

        Node replacement;
        if (z.left == null) {
            replacement = z.right;
            transplant(z, z.right);
            x = replacement;
            xParent = (replacement != null) ? replacement.parent : z.parent;
        } else if (z.right == null) {
            replacement = z.left;
            transplant(z, z.left);
            x = replacement;
            xParent = (replacement != null) ? replacement.parent : z.parent;
        } else {
            y = minimum(z.right);
            yOriginalColor = y.color;
            x = y.right;
            if (y.parent == z) {
                xParent = y;
            } else {
                transplant(y, y.right);
                y.right = z.right;
                if (y.right != null) y.right.parent = y;
                xParent = y.parent;
            }
            transplant(z, y);
            y.left = z.left;
            if (y.left != null) y.left.parent = y;
            y.color = z.color;
        }
        if (yOriginalColor == BLACK) deleteFixup(x, xParent);
    }

    private void deleteFixup(Node x, Node xParent) {
        while ((x != root) && colorOf(x) == BLACK) {
            if (x == leftOf(xParent)) {
                Node w = rightOf(xParent);
                if (colorOf(w) == RED) {
                    w.color = BLACK;
                    xParent.color = RED;
                    leftRotate(xParent);
                    w = rightOf(xParent);
                }
                if (colorOf(leftOf(w)) == BLACK && colorOf(rightOf(w)) == BLACK) {
                    if (w != null) w.color = RED;
                    x = xParent;
                    xParent = x.parent;
                } else {
                    if (colorOf(rightOf(w)) == BLACK) {
                        if (leftOf(w) != null) leftOf(w).color = BLACK;
                        if (w != null) w.color = RED;
                        rightRotate(w);
                        w = rightOf(xParent);
                    }
                    if (w != null) w.color = colorOf(xParent);
                    xParent.color = BLACK;
                    if (rightOf(w) != null) rightOf(w).color = BLACK;
                    leftRotate(xParent);
                    x = root;
                }
            } else {
                Node w = leftOf(xParent);
                if (colorOf(w) == RED) {
                    w.color = BLACK;
                    xParent.color = RED;
                    rightRotate(xParent);
                    w = leftOf(xParent);
                }
                if (colorOf(rightOf(w)) == BLACK && colorOf(leftOf(w)) == BLACK) {
                    if (w != null) w.color = RED;
                    x = xParent;
                    xParent = x.parent;
                } else {
                    if (colorOf(leftOf(w)) == BLACK) {
                        if (rightOf(w) != null) rightOf(w).color = BLACK;
                        if (w != null) w.color = RED;
                        leftRotate(w);
                        w = leftOf(xParent);
                    }
                    if (w != null) w.color = colorOf(xParent);
                    xParent.color = BLACK;
                    if (leftOf(w) != null) leftOf(w).color = BLACK;
                    rightRotate(xParent);
                    x = root;
                }
            }
        }
        if (x != null) x.color = BLACK;
    }

    // ---- small RB accessors ----
    private boolean colorOf(Node n) { return n == null ? BLACK : n.color; }
    private Node leftOf(Node n) { return n == null ? null : n.left; }
    private Node rightOf(Node n) { return n == null ? null : n.right; }

    private void transplant(Node u, Node v) {
        if (u.parent == null) root = v;
        else if (u == u.parent.left) u.parent.left = v;
        else u.parent.right = v;
        if (v != null) v.parent = u.parent;
    }

    private Node minimum(Node x) {
        while (x.left != null) x = x.left;
        return x;
    }
}
