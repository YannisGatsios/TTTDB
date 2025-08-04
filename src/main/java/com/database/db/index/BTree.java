package com.database.db.index;

import java.util.List;

/**
 * Interface defining core operations for a B+ Tree structure.
 *
 * @param <K> Key type (must implement Comparable)
 * @param <V> Value type associated with keys
 */
public interface BTree<K extends Comparable<? super K>, V> {

    /**
     * Inserts a key-value pair into the tree.
     *
     * @param key Key to insert
     * @param value Associated value
     */
    void insert(K key, V value);

    /**
     * Removes a specific key-value pair from the tree.
     *
     * @param key Key to remove
     * @param value Specific value to remove (for duplicate keys if is duplicate)
     */
    void remove(K key, V value);

    /**
     * Searches for a key and returns its key-value pair.
     *
     * @param key Key to search for
     * @return Pair containing key and value, or null if not found
     */
    List<V> search(K key);

    /**
     * Performs a range search between two keys (inclusive).
     *
     * @param fromKey Lower bound (inclusive, null for unbounded)
     * @param toKey Upper bound (inclusive, null for unbounded)
     * @return List of key-value pairs within the specified range
     */
    List<Pair<K,V>> rangeSearch(K fromKey, K toKey);

    /**
     * Checks if a key exists in the tree.
     *
     * @param key Key to verify
     * @return true if key exists, false otherwise
     */
    boolean isKey(K key);

    /**
     * Generates a human-readable tree representation.
     *
     * @return String visualization of tree structure and leaf chain
     */
    @Override
    String toString();
}