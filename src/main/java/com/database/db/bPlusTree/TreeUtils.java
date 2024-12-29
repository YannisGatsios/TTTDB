package com.database.db.bPlusTree;

public class TreeUtils {
    public static class Pair<K, V> {
        private final K key;
        private final V value;
    
        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }
    
        public K getKey() {
            return key;
        }
    
        public V getValue() {
            return value;
        }
    
        @Override
        public String toString() {
            return "Pair{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    } 
}
