package com.database.db.page;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class Cache {
    private static final Logger logger = Logger.getLogger(Cache.class.getName());

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
                        writePage(eldest.getValue());
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, String.format("Failed to evict page ID %d from table '%s'.", eldest.getKey(), table.getName()), e);
                        throw new RuntimeException("Eviction failed for page ID: " + eldest.getKey(), e);
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /** Write the given page to disk if needed and remove it from cache. */
    public void writePage(TablePage page) throws IOException {
        if (page.isDirty()) {
            fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
            counter++;
            if (counter >= capacity) {
                try {
                    table.getIndexManager().writeIndexes(table);
                } catch (ExecutionException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                counter = 0;
            }
        }
    }

    /** Write all pages in cache to disk and clear cache. */
    private synchronized void writeCache() {
        Map<Integer, TablePage> sortedCache = new TreeMap<>(cache); // deterministic eviction
        for (Map.Entry<Integer, TablePage> entry : sortedCache.entrySet()) {
            TablePage page = entry.getValue();
            if (page.isDirty()) {
                try {
                    fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
                } catch (IOException e) {
                    logger.log(Level.WARNING, String.format("Failed to write page ID %d to disk for table '%s'.", entry.getKey(), table.getName()), e);
                }
            }
        }
        this.cache.clear();
        logger.info(String.format("Cache flushed and cleared for table '%s'.", table.getName()));
    }

    /** Load a page into cache, evicting LRU if necessary. */
    public TablePage loadPage(int pageID) {
        TablePage newPage = new TablePage(pageID, table);
        try {
            byte[] pageBuffer = fileIO.readPage(table.getPath(), newPage.getPagePos(),newPage.sizeInBytes());
            if(pageBuffer != null) newPage.fromBytes(pageBuffer);
            cache.put(pageID, newPage);
            return newPage;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, String.format("Interrupted while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, String.format("Execution failed while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        }
        return null;
    }

    public synchronized TablePage get(int pageID) {
        TablePage page = cache.get(pageID);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format("Cache miss for page ID %d in table '%s'. Loading...", pageID, table.getName()));
            return loadPage(pageID);
        }
    }

    public synchronized TablePage getLast(){
        int lastPageId = table.getPages() - 1;
        if (lastPageId == -1) lastPageId = 0;
        return this.get(lastPageId);
    }

    public synchronized void put(TablePage page) {
        cache.put(page.getPageID(), page);
    }

    public synchronized TablePage remove(int pageID) {
        return cache.remove(pageID);
    }

    public synchronized boolean containsKey(int pageID) {
        return cache.containsKey(pageID);
    }

    public synchronized void clear(){
        try {
            table.getIndexManager().writeIndexes(table);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while writing indexes for table '" + table.getName() + "'", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt status
            logger.log(Level.SEVERE, "InterruptedException while writing indexes for table '" + table.getName() + "'",
                    e);
        }
        writeCache();
    }

    public void deleteLastPage(TablePage page) {
        this.cache.remove(page.getPageID());
        FileIO fileIO = new FileIO(table.getFileIOThread());
        try {
            fileIO.truncateFile(table.getPath(), page.sizeInBytes());
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing last page.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing last page.", e);
            Thread.currentThread().interrupt(); // good practice to reset interrupt status
        }
        table.removeOnePage();
    }
}
