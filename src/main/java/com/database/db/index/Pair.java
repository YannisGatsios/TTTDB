package com.database.db.index;

public class Pair<K, V> {
    K key;
    V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        String keyString;
        if(key instanceof byte[]){
            StringBuilder sb = new StringBuilder();
            for (byte b : (byte[])key) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(b & 0xFF);
            }
            keyString = sb.toString();
        }
        keyString = key.toString();
        return "[" + keyString + ", " + value + "]";
    }
}
