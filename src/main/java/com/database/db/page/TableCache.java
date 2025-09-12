package com.database.db.page;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class TableCache {
    private static final Logger logger = Logger.getLogger(TableCache.class.getName());

    private final FileIO fileIO;
    private final Table table;
    private int truncationCount = 0;
    private final Set<Integer> deletedPages = ConcurrentHashMap.newKeySet();

    // LRU cache using LinkedHashMap
    private final Map<Integer, TablePage> cache;

    private int capacity;

    public TableCache(Table table, int capacity, FileIO fileIO){
        this.fileIO = fileIO;
        this.table = table;
        this.capacity = capacity;
        if(capacity == -1) this.cache = new HashMap<>();
        else{
            this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, TablePage> eldest) {
                    if (size() > capacity) {
                        writePage(eldest.getValue());
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    /** Write the given page to disk if needed and remove it from cache. */
    public void writePage(TablePage page){
        if (page.isDirty()) {
            fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
        }
    }

    /** Write all pages in cache to disk and clear cache. */
    public synchronized void writeCache() {
        Map<Integer, TablePage> sortedCache = new TreeMap<>(cache); // deterministic eviction
        for (Map.Entry<Integer, TablePage> entry : sortedCache.entrySet()) {
            TablePage page = entry.getValue();
            if (page.isDirty()) {
                fileIO.writePage(table.getPath(), page.toBytes(), page.getPagePos());
            }
        }
        this.cache.clear();
        if(truncationCount > 0) truncate();
        logger.info(String.format("Cache flushed and cleared for table '%s'.", table.getName()));
    }

    public void clear(){
        this.cache.clear();
    }

    /** Load a page into cache, evicting LRU if necessary. */
    public TablePage loadPage(int pageID) {
        TablePage newPage = new TablePage(pageID, table);
        if(deletedPages.contains(pageID)){
            deletedPages.remove(pageID);
            cache.put(pageID, newPage);
            if(truncationCount > 0)truncationCount--;
            return newPage;
        }
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

    public void deleteLastPage(TablePage page) {
        this.cache.remove(page.getPageID());
        deletedPages.add(page.getPageID());
        table.removeOnePage();
        truncationCount++;
        if(capacity<0)return;
        if(truncationCount >= capacity) truncate();
    }
    private void truncate(){
        if (truncationCount == 0) return;
        try {
            fileIO.truncateFile(table.getPath(), truncationCount*Page.pageSizeInBytes(TablePage.sizeOfEntry(table)));
            truncationCount = 0;
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing last page.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing last page.", e);
            Thread.currentThread().interrupt(); // good practice to reset interrupt status
        }
    }
}