package com.database.db.bPlusTree;

import java.util.List;

/**
 * Interface for a B+ tree data structure.
 *
 * @param <K> the type of keys, which must be comparable
 * @param <V> the type of values associated with the keys
 */
public interface BTree<K extends Comparable<K>, V> {

    /**
     * Inserts a key-value pair into the B+ tree.
     *
     * @param key   the key to insert
     * @param value the value associated with the key
     */
    void insert(K key, V value);

    /**
     * Removes a key and its associated value from the B+ tree.
     *
     * @param key the key to remove
     * @return true if the key was successfully removed, false otherwise
     */
    void remove(K key);

    /**
     * Searches for the value associated with a specific key in the B+ tree.
     *
     * @param key the key to search for
     * @return the value associated with the key, or null if the key is not found
     */
    V search(K key);

    /**
     * Retrieves all key-value pairs within a specified range in the B+ tree.
     *
     * @param fromKey the start of the range (inclusive)
     * @param toKey   the end of the range (inclusive)
     * @return a list of key-value pairs in the specified range as Pair<K, V>
     */
    List<Pair<K, V>> rangeSearch(K fromKey, K toKey);

    /**
     * Checks if a specific key exists in the B+ tree.
     *
     * @param key the key to check for
     * @return true if the key exists, false otherwise
     */
    boolean isKey(K key);

    /**
     * Prints the structure of the B+ tree, providing a visual representation for debugging.
     */
    void printTree();
}
