package com.database.db.index;

import java.util.List;
/**
 * Interface defining core operations for an index structure.
 * This can be implemented using different data structures like
 * B+Tree, Hash Table, Skip List, AVL/Red-Black Tree, Trie, etc.
 *
 * @param <K> Key type (must implement Comparable)
 * @param <V> Value type associated with keys
 */
public interface Index<K extends Comparable<? super K>, V> {

    /**
     * Inserts a key-value pair into the index.
     *
     * @param key Key to insert
     * @param value Associated value
     */
    void insert(K key, V value);

    /**
     * Removes a specific key-value pair from the index.
     *
     * @param key Key to remove
     * @param value Specific value to remove (useful if duplicate keys exist)
     */
    void remove(K key, V value);

    /**
     * Searches for a key and returns all associated values.
     *
     * @param key Key to search for
     * @return List of key-value pairs matching the key, or empty list if not found
     */
    List<Pair<K, V>> search(K key);

    /**
     * Performs a range search between two keys (inclusive).
     * If a particular implementation does not support range queries, it can return all matching keys or throw UnsupportedOperationException.
     *
     * @param fromKey Lower bound (inclusive, null for unbounded)
     * @param toKey Upper bound (inclusive, null for unbounded)
     * @return List of key-value pairs within the specified range
     */
    List<Pair<K, V>> rangeSearch(K fromKey, K toKey);

    /**
     * Checks if a key exists in the index.
     *
     * @param key Key to check
     * @return true if key exists, false otherwise
     */
    boolean isKey(K key);

    /**
     * Updates a value associated with a key (assumes unique keys).
     *
     * @param key Key to update
     * @param newValue New value to set
     */
    void update(K key, V newValue);

    /**
     * Updates a value associated with a key. For non-unique keys,
     * specifies the old value to replace.
     *
     * @param key Key to update
     * @param newValue New value to set
     * @param oldValue Old value to replace (required for non-unique keys)
     */
    void update(K key, V newValue, V oldValue);

    /**
     * Returns a human-readable representation of the index.
     * The exact format is implementation-specific.
     *
     * @return String visualization of index contents
     */
    @Override
    String toString();
}