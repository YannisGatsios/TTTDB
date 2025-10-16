package com.database.tttdb.core.index.btree;

import java.util.ArrayList;
import java.util.List;

import com.database.tttdb.core.index.Pair;
/**
 * Represents a node in the B+ Tree structure (either internal or leaf node).
 *
 * <p>Node Type Characteristics:
 * <ul>
 *   <li>Leaf Nodes: Store key-value pairs and maintain linked-list pointers</li>
 *   <li>Internal Nodes: Store separator keys and child pointers</li>
 * </ul>
 *
 * @param <K> Key type (must be comparable)
 * @param <V> Value type associated with keys
 */
public class Node<K,V> {
    public boolean isLeaf;        // Node type identifier
    public List<Pair<K,V>> pairs; // Keys + values (leaf) or separator keys (internal)
    public List<Node<K,V>> children;  // Child nodes (internal nodes only)
    public Node<K,V> parent;      // Parent node reference
    public Node<K,V> next;        // Next leaf node pointer (leaf nodes only)

    /**
     * Creates a new node of specified type.
     *
     * @param isLeaf true for leaf node, false for internal node
     */
    public Node(Boolean isLeaf){
        this.isLeaf = isLeaf;
        this.pairs = new ArrayList<>();
        this.children = isLeaf ? null : new ArrayList<>();
        this.parent = null;
        this.next = null;
    }

    /**
     * Traverses to the leftmost leaf in the subtree and retrieves its first key.
     * Used for updating parent separator keys during deletions.
     *
     * @return First key in the leftmost leaf, or null if no keys exist
     */
    public K getFirstLeafKey() {
        Node<K, V> n = this;
        while (!n.isLeaf) {
            n = n.children.getFirst();
        }
        return n.pairs.isEmpty() ? null : n.pairs.getFirst().key;
    }

    /**
     * Generates a structural summary:
     * - Lists all keys with their values
     * - Indicates node type (leaf/internal)
     * - Shows child count (internal nodes)
     *
     * @return Formatted node information
     */
    @Override
    public String toString() {
        StringBuilder keys = new StringBuilder("{Keys:[");
        for(Pair<K,V> pair : this.pairs){
            keys.append(pair.toString()).append(", ");
        }
        String stats = "], Leaf:"+ (isLeaf ? "Yes":"No") + ", ChildrenNum:" + (this.children == null ? "NULL":this.children.size());
        return "\n"+keys+stats+"}\n";
    }
}
