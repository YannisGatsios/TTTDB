package com.database.db.page;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class Cache {
    private final FileIO fileIO;
    private final int capacity;
    private final Table table;
    private int counter = 0;

    // LRU cache using LinkedHashMap
    private final Map<Integer, TablePage> cache;

    public Cache(Table table) {
        this.fileIO = new FileIO(table.getFileIOThread());
        this.capacity = table.CACHE_CAPACITY;
        this.table = table;

        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, TablePage> eldest) {
                if (size() > capacity) {
                    try {
                        evictPage(eldest.getKey(), eldest.getValue());
                    } catch (IOException e) {
                        throw new RuntimeException(e); // or handle properly
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /** Write the given page to disk if needed and remove it from cache. */
    private void evictPage(int pageID, TablePage page) throws IOException {
        if (page.isDirty()) { // assume TablePage has a dirty flag
            fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
            this.counter++;
            if(this.counter >= this.capacity){
                
            }
        }
        // no need to remove explicitly, LinkedHashMap does it
    }

    /** Write all pages in cache to disk and clear cache. */
    public synchronized void writeCache() throws IOException {
        Map<Integer, TablePage> sortedCache = new TreeMap<>(cache);
        for (Map.Entry<Integer, TablePage> entry : sortedCache.entrySet()) {
            TablePage page = entry.getValue();
            if (page.isDirty()) {
                fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
            }
        }
        cache.clear();
    }

    /** Load a page into cache, evicting LRU if necessary. */
    public   TablePage loadPage(int pageID) throws IOException, InterruptedException, ExecutionException {
        TablePage page = cache.get(pageID);
        if (page != null) {
            return page;
        }

        TablePage newPage = new TablePage(pageID, table);
        cache.put(pageID, newPage);
        return newPage;
    }

    public synchronized TablePage get(int pageID) {
        TablePage page = cache.get(pageID);
        if (page == null) {
            try {
                page = loadPage(pageID);
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to load page " + pageID, e);
            }
        }
        return page;
    }

    public synchronized TablePage getLast() throws IOException, ExecutionException, InterruptedException {
        int lastPageId = table.getPages() - 1;
        return this.get(lastPageId);
    }

    public synchronized void put(int pageID, TablePage page) {
        cache.put(pageID, page);
    }

    public synchronized TablePage remove(int pageID) {
        return cache.remove(pageID);
    }

    public synchronized boolean containsKey(int pageID) {
        return cache.containsKey(pageID);
    }

    public synchronized void clear() throws IOException {
        writeCache();
    }
}
