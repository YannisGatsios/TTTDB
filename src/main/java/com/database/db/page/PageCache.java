package com.database.db.page;

import java.util.LinkedHashMap;
import java.util.Map;

public class PageCache<K> {
    private final int maxSize;
    private final Map<Integer,Page<K>> cache;

    public PageCache(int maxSize){
        this.maxSize = maxSize;
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page<K>> eldest) {
                return size() > PageCache.this.maxSize;
            }
        };
    }

    public synchronized Page<K> get(int pageID){
        return cache.get(pageID);
    }

    public synchronized void put(int pageID, Page<K> page){
        this.cache.put(pageID, page);
    }

    public synchronized Page<K> remove(int pageID){
        return this.cache.remove(pageID);
    }

    public synchronized boolean containsKey(int pageID){
        return this.cache.containsKey(pageID);
    }

    public synchronized void clear(){
        this.cache.clear();
    }
}
