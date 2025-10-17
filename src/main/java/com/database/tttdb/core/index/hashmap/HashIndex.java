package com.database.tttdb.core.index.hashmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.Pair;

public class HashIndex<K extends Comparable<? super K>,V> implements Index<K,V> {

    private final HashMap<K,List<V>> hashmap;
    private boolean isNullable = false;
    private boolean isUnique = true;
    private long size = 0;

    public HashIndex(){
        this.hashmap = new HashMap<>();
    }

    public void insert(K key, V value) {
        ensureKeyValidity(key);
        if (isUnique) {
            List<V> list = hashmap.get(key);
            if (list == null) {
                list = new ArrayList<>(1);
                list.add(value);
                hashmap.put(key, list);
                size++;
            } else {
                list.set(0, value);
            }
        } else {
            List<V> list = hashmap.computeIfAbsent(key, k -> new ArrayList<>());
            list.add(value);
            size++;
        }
    }

    public void remove(K key, V value) {
        ensureKeyValidity(key);
        List<V> list = hashmap.get(key);
        if (list == null) return;
        if (value == null) {
            if (list.remove(null)) size--;
        } else {
            if(list.remove(value)) size--;
        }
        if (list.isEmpty()) {
            hashmap.remove(key);
        }
    }

    public List<Pair<K, V>> search(K key) {
        ensureKeyValidity(key);
        List<Pair<K, V>> out = new ArrayList<>();
        List<V> list = hashmap.get(key);
        if (list == null) return out;
        for (V v : list) {
            out.add(new Pair<>(key, v));
        }
        return out;
    }

    public List<Pair<K, V>> rangeSearch(K fromKey, K toKey) {
        // Unordered index: full scan with filtering. Bounds inclusive. Null means unbounded.
        List<Pair<K, V>> out = new ArrayList<>();
        for (Map.Entry<K, List<V>> e : hashmap.entrySet()) {
            K k = e.getKey();
            if (!within(k, fromKey, toKey)) continue;
            for (V v : e.getValue()) {
                out.add(new Pair<>(k, v));
            }
        }
        return out;
    }

    private boolean within(K k, K from, K to) {
        if (k == null) {
            // allow null keys only when enabled and bounds do not exclude them
            return isNullable && (from == null) && (to == null);
        }
        boolean geFrom = (from == null) || k.compareTo(from) >= 0;
        boolean leTo = (to == null) || k.compareTo(to) <= 0;
        return geFrom && leTo;
    }

    public boolean isKey(K key) {
        ensureKeyValidity(key);
        List<V> list = hashmap.get(key);
        return list != null && !list.isEmpty();
    }

    public void update(K key, V newValue) {
        ensureKeyValidity(key);
        if (!isUnique) {
            throw new IllegalStateException("update(key,newValue) requires unique index");
        }
        List<V> list = hashmap.get(key);
        if (list == null || list.isEmpty()) {
            throw new NoSuchElementException("Key not found");
        }
        list.set(0, newValue);
    }

    public void update(K key, V newValue, V oldValue) {
        ensureKeyValidity(key);
        if (isUnique) {
            throw new IllegalStateException("update(key,newValue,oldValue) is for non-unique index");
        }
        List<V> list = hashmap.get(key);
        if (list == null || list.isEmpty()) {
            throw new NoSuchElementException("Key not found");
        }
        boolean replaced = false;
        if (oldValue == null) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) == null) {
                    list.set(i, newValue);
                    replaced = true;
                    break;
                }
            }
        } else {
            for (int i = 0; i < list.size(); i++) {
                if (oldValue.equals(list.get(i))) {
                    list.set(i, newValue);
                    replaced = true;
                    break;
                }
            }
        }
        if (!replaced) {
            throw new NoSuchElementException("Old value not found for key");
        }
    }

    public long size(){
        return size;
    }

    public K getMax() {
        K max = null;
        for (K k : hashmap.keySet()) {
            if (k == null) continue; // null excluded from ordering
            if (max == null || k.compareTo(max) > 0) {
                max = k;
            }
        }
        return max; // may be null if empty or only null keys exist
    }

    public void setUnique(boolean isUnique) { this.isUnique = isUnique; }
    public void setNullable(boolean isNullable) { this.isNullable = isNullable; }
    public boolean isUnique() { return isUnique; }
    public boolean isNullable() { return isNullable; }

    private void ensureKeyValidity(K key) {
        if (key == null && !isNullable) {
            throw new IllegalArgumentException("Null keys are not allowed");
        }
    }
}