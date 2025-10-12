package com.database.tttdb.core.index;

import java.util.List;
/**
 * Interface defining the core operations for an index structure.
 * Implementations may use different underlying data structures such as:
 * <ul>
 *     <li>B+ Tree – ordered, supports efficient range queries.</li>
 *     <li>Hash Table – unordered, optimized for exact key lookups.</li>
 *     <li>Skip List – probabilistic balanced structure with range support.</li>
 *     <li>AVL / Red-Black Tree – self-balancing binary search trees.</li>
 *     <li>Trie – prefix-based index for string keys.</li>
 * </ul>
 *
 * <p>This abstraction allows flexible index implementations to be
 * interchangeable within the database system.</p>
 *
 * @param <K> Key type (must implement {@link Comparable})
 * @param <V> Value type associated with keys
 */
public interface Index<K extends Comparable<? super K>, V> {

    /**
     * Inserts a key-value pair into the index.
     * Implementations must handle duplicate keys according to
     * {@link #isUnique()} configuration.
     *
     * @param key Key to insert
     * @param value Associated value
     */
    void insert(K key, V value);

    /**
     * Removes a specific key-value pair from the index.
     * If multiple identical values exist for a key, only the specified
     * pair is removed.
     *
     * @param key Key to remove
     * @param value Specific value to remove
     */
    void remove(K key, V value);

    /**
     * Searches for a key and returns all associated key-value pairs.
     *
     * @param key Key to search for
     * @return List of key-value pairs matching the key, or an empty list if not found
     */
    List<Pair<K, V>> search(K key);

    /**
     * Performs a range search between two keys (inclusive).
     * <p>
     * If the underlying structure supports ordering (e.g., B+Tree, Skip List),
     * the operation is efficient.  
     * For unordered structures (e.g., hash-based indexes), this method
     * performs a full scan and filters entries within the range.
     * </p>
     *
     * <p>
     * Both {@code fromKey} and {@code toKey} may be {@code null}:
     * <ul>
     *     <li>{@code fromKey == null} → unbounded lower limit</li>
     *     <li>{@code toKey == null} → unbounded upper limit</li>
     * </ul>
     * </p>
     *
     * @param fromKey Lower bound (inclusive), {@code null} for unbounded
     * @param toKey Upper bound (inclusive), {@code null} for unbounded
     * @return List of key-value pairs within the specified range.
     *         May be O(n) for unordered indexes.
     */
    List<Pair<K, V>> rangeSearch(K fromKey, K toKey);

    /**
     * Checks whether a specific key exists in the index.
     *
     * @param key Key to check
     * @return {@code true} if the key exists, {@code false} otherwise
     */
    boolean isKey(K key);

    /**
     * Updates the value associated with a key.
     * Assumes the index enforces unique keys.
     *
     * @param key Key whose value is to be updated
     * @param newValue New value to associate with the key
     */
    void update(K key, V newValue);

    /**
     * Updates a value associated with a key in a non-unique index.
     * Replaces a specific old value with a new one.
     *
     * @param key Key whose value is to be updated
     * @param newValue New value to set
     * @param oldValue Old value to replace
     */
    void update(K key, V newValue, V oldValue);

    /**
     * Returns the maximum key stored in the index.
     * If the index is empty, behavior is implementation-defined
     * (may return {@code null} or throw an exception).
     *
     * @return Maximum key
     */
    K getMax();

    /**
     * Defines whether the index enforces unique keys.
     *
     * @param isUnique {@code true} if unique, {@code false} otherwise
     */
    void setUnique(boolean isUnique);

    /**
     * Defines whether the index allows {@code null} keys.
     *
     * @param isNullable {@code true} if {@code null} keys are allowed
     */
    void setNullable(boolean isNullable);

    /**
     * Returns whether the index enforces unique keys.
     *
     * @return {@code true} if unique, {@code false} otherwise
     */
    boolean isUnique();

    /**
     * Returns whether the index allows {@code null} keys.
     *
     * @return {@code true} if {@code null} keys are allowed
     */
    boolean isNullable();

    /**
     * Returns a human-readable string representation of the index.
     * This may include key-value pairs or structure details depending
     * on implementation.
     *
     * @return String visualization of the index contents
     */
    @Override
    String toString();
}
