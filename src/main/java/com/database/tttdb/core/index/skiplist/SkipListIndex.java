package com.database.tttdb.core.index.skiplist;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.database.tttdb.core.index.Index;
import com.database.tttdb.core.index.Pair;

public final class SkipListIndex<K extends Comparable<? super K>, V> implements Index<K, V> {
    private static final int MAX_LEVEL = 32;
    private final Node<K,V> head = new Node<>(null, MAX_LEVEL);
    private int level = 1;
    private long size = 0;

    private boolean unique = false;
    private boolean nullable = false;

    // null-key bucket
    private LinkedHashSet<V> nullValues = null;

    private static final class Node<K,V> {
        final K key;
        final Node<K,V>[] next;
        final LinkedHashSet<V> values = new LinkedHashSet<>();
        final int topLevel; // number of usable levels

        @SuppressWarnings("unchecked")
        Node(K key, int topLevel) {
            this.key = key;
            this.topLevel = topLevel;                // in [1..MAX_LEVEL]
            this.next = (Node<K,V>[]) new Node[MAX_LEVEL]; // fixed size
        }
    }
    private int cmp(K a, K b) {
        return a.compareTo(b);
    }
    private int randomLevel() {
        int lvl = 1;
        while (lvl < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < 0.5) lvl++;
        return lvl;
    }

    private Node<K,V> findNode(K key) {
        Node<K,V> node = head;
        for (int i = level - 1; i >= 0; i--) {
            while (node.next[i] != null) {
                int c = cmp(node.next[i].key, key);
                if (c < 0) node = node.next[i]; else break;
            }
        }
        node = node.next[0];
        return (node != null && cmp(node.key, key) == 0) ? node : null;
    }

    @SuppressWarnings("unchecked")
    private Node<K,V>[] findPreds(K key) {
        Node<K,V> node = head;
        Node<K,V>[] preds = (Node<K,V>[]) new Node[level];
        for (int i = level - 1; i >= 0; i--) {
            while (node.next[i] != null && cmp(node.next[i].key, key) < 0) node = node.next[i];
            preds[i] = node;
        }
        return preds;
    }

    public void insert(K key, V value) {
            if (key == null) {
        if (!nullable) throw new IllegalArgumentException("Null keys not allowed");
        if (nullValues == null) nullValues = new LinkedHashSet<>();
        if (unique) {
            if (nullValues.isEmpty()) { nullValues.add(value); size++; }
            else if (!nullValues.contains(value)) { nullValues.clear(); nullValues.add(value); }
        } else {
            if (nullValues.add(value)) size++;
        }
        return;
    }
    Node<K,V> node = findNode(key);
        if (node != null) {
            if (unique) {
                if (node.values.isEmpty()) { node.values.add(value); size++; }
                else if (!node.values.contains(value)) { node.values.clear(); node.values.add(value); }
            } else {
                if (node.values.add(value)) size++;
            }
            return;
        }
        Node<K,V>[] preds = findPreds(key);
        int newLvl = randomLevel();
        if (newLvl > level) {
            @SuppressWarnings("unchecked")
            Node<K,V>[] grown = (Node<K,V>[]) new Node[newLvl];
            System.arraycopy(preds, 0, grown, 0, preds.length);
            for (int i = level; i < newLvl; i++) grown[i] = head;
            preds = grown;
            level = newLvl;
        }
        Node<K,V> n = new Node<>(key, newLvl);
        n.values.add(value);
        size++;
        for (int i = 0; i < newLvl; i++) {
            n.next[i] = preds[i].next[i];
            preds[i].next[i] = n;
        }
    }

    public void remove(K key, V value) {
        if (key == null) {
            if (!nullable || nullValues == null) return;
            if (nullValues.remove(value)) {
                size--;
                if (nullValues.isEmpty()) nullValues = null;
            }
            return;
        }
        Node<K,V>[] preds = findPreds(key);
        Node<K,V> node = preds[0].next[0];
        if (node == null || cmp(node.key, key) != 0) return;

        if (!node.values.remove(value)) return;
        size--;
        if (!node.values.isEmpty()) return;
        for (int i = 0; i < node.topLevel; i++) {
            if (preds[i].next[i] == node) preds[i].next[i] = node.next[i];
        }
        while (level > 1 && head.next[level - 1] == null) level--;
    }

    public List<Pair<K, V>> search(K key) {
        if (key == null) {
            if (!nullable || nullValues == null) return List.of();
            return valuesToPairs(null, nullValues);
        }
        Node<K,V> node = findNode(key);
        if (node == null) return List.of();
        return valuesToPairs(node.key, node.values);
    }

    public List<Pair<K,V>> rangeSearch(K fromKey, K toKey) {
        List<Pair<K,V>> out = new ArrayList<>();
        if (nullable && nullValues != null && fromKey == null) out.addAll(valuesToPairs(null, nullValues));

        Node<K,V> node = head;
        if (fromKey != null) {
            for (int i = level - 1; i >= 0; i--) {
                while (node.next[i] != null && cmp(node.next[i].key, fromKey) < 0) node = node.next[i];
            }
            node = node.next[0];
        } else {
            node = head.next[0];
        }
        while (node != null && (toKey == null || cmp(node.key, toKey) <= 0)) {
            out.addAll(valuesToPairs(node.key, node.values));
            node = node.next[0];
        }
        return out;
    }

    public boolean isKey(K key) {
        if (key == null) return nullable && nullValues != null && !nullValues.isEmpty();
        return findNode(key) != null;
    }

    public void update(K key, V newValue) {
        if (!unique) throw new IllegalStateException("update(key,newValue) requires unique index");
        if (key == null) {
            if (!nullable || nullValues == null || nullValues.isEmpty())
                throw new NoSuchElementException("Key not found");
            nullValues.clear();
            nullValues.add(newValue);
            return;
        }
        Node<K,V> node = findNode(key);
        if (node == null) throw new NoSuchElementException("Key not found");
        node.values.clear();
        node.values.add(newValue);
    }

    @Override
    public void update(K key, V newValue, V oldValue) {
        if (key == null) {
            if (!nullable || nullValues == null || !nullValues.remove(oldValue))
                throw new NoSuchElementException("Pair not found");
            nullValues.add(newValue);
            return;
        }
        Node<K,V> n = findNode(key);
        if (n == null || !n.values.remove(oldValue))
            throw new NoSuchElementException("Pair not found");
        n.values.add(newValue);
    }

    public long size(){
        return size;
    }

    public K getMax() {
        Node<K,V> x = head;
        for (int i = level - 1; i >= 0; i--) {
            while (x.next[i] != null) x = x.next[i];
        }
        return x == head ? null : x.key;
    }

    public void setUnique(boolean isUnique) { this.unique = isUnique; }
    public void setNullable(boolean isNullable) { this.nullable = isNullable; }
    public boolean isUnique() { return unique; }
    public boolean isNullable() { return nullable; }

    private List<Pair<K,V>> valuesToPairs(K key, Collection<V> values) {
        List<Pair<K,V>> res = new ArrayList<>(values.size());
        // Respect Pair’s primary+duplicates model when possible
        Iterator<V> it = values.iterator();
        if (!it.hasNext()) return res;
        V first = it.next();
        Pair<K,V> p = new Pair<>(key, first);
        while (it.hasNext()) p.addDup(it.next());
        res.addAll(p.getAllPairs());
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (nullable && nullValues != null && !nullValues.isEmpty()) {
            for (V v : nullValues) sb.append(new Pair<K,V>(null, v)).append('\n');
        }
        Node<K,V> node = head.next[0];
        while (node != null) {
            for (V v : node.values) sb.append(new Pair<>(node.key, v)).append('\n');
            node = node.next[0];
        }
        return sb.toString();
    }
}