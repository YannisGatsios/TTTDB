package com.database.db.index;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents a key-value pair in a B+ Tree with support for duplicate values.
 *
 * <p>When the tree is configured for non-unique keys, duplicate values are stored
 * in a separate set. Primary values and duplicates are treated equally for search
 * operations.</p>
 *
 * @param <K> Key type (must be comparable)
 * @param <V> Value type associated with the key
 */
public class Pair<K, V> {
    public K key;
    public V value;
    private Set<V> duplicate;//Used to store duplicate values when B+Tree is not set to unique.
    /**
     * Creates a new key-value pair.
     *
     * @param key   The key for the pair
     * @param value The primary value associated with the key
     */
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
        this.duplicate = null;
    }
    /**
     * Adds a duplicate value for this key.
     *
     * @param value Duplicate value to add
     */
    public void addDup(V value){
        if(this.duplicate == null) this.duplicate = new HashSet<>();
        this.duplicate.add(value);
    }
    /**
     * Removes a duplicate value.
     *
     * @param value Duplicate value to remove
     */
    public void removeDup(V value){
        this.duplicate.remove(value);
        if(this.duplicate.isEmpty()){
            this.duplicate = null;
        }
    }
    /**
     * @return Set of duplicate values (null if none exist)
     */
    public Set<V> getDuplicates(){return this.duplicate;}
    /**
     * Checks if a value exists (either as primary or duplicate).
     *
     * @param Value Value to search for
     * @return The value if found, otherwise null
     */
    public V get(V Value){
        if(Value.equals(this.value)) return this.value;
        if(this.duplicate.contains(Value)) return Value;
        return null;
    }
    /**
     * Generates a human-readable representation:
     * - Byte arrays are displayed as space-separated integers
     * - Shows duplicate count (e.g., "D3" for 3 duplicates)
     *
     * @return Formatted string representation
     */
    @Override
    public String toString() {
        String keyString;
        if(key instanceof byte[]){
            StringBuilder sb = new StringBuilder();
            for (byte b : (byte[])key) {
                if (!sb.isEmpty()) {
                    sb.append(" ");
                }
                sb.append(b & 0xFF);
            }
            keyString = sb.toString();
        }else{
            keyString = key.toString();
        }
        String dup = this.duplicate == null ? "" : "D"+String.valueOf(this.duplicate.size());
        return "[Key=" + keyString + ", Value=" + value + "]"+dup;
    }
}