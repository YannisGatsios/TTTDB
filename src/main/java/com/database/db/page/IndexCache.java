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

public class IndexCache {
    private static final Logger logger = Logger.getLogger(Cache.class.getName());

    private final FileIO fileIO;
    private final Table table;
    private final int columnIndex;

    // LRU cache using LinkedHashMap
    private final Map<Integer, IndexPage> cache;

    public IndexCache(Table table, int capacity, FileIO fileIO, int columnIndex){
        this.fileIO = fileIO;
        this.table = table;
        this.columnIndex = columnIndex;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, IndexPage> eldest) {
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

    public void writePage(IndexPage page) throws IOException {
        if (page.isDirty()) {
            fileIO.writePage(table.getIndexPath(columnIndex), page.toBytes(), page.getPagePos());
        }
    }

    /** Write all pages in cache to disk and clear cache. */
    public synchronized void writeCache() {
        Map<Integer, IndexPage> sortedCache = new TreeMap<>(cache); // deterministic eviction
        for (Map.Entry<Integer, IndexPage> entry : sortedCache.entrySet()) {
            IndexPage page = entry.getValue();
            if (page.isDirty()) {
                fileIO.writePage(table.getIndexPath(columnIndex), page.toBytes(), page.getPagePos());
            }
        }
        this.cache.clear();
        logger.info(String.format("Cache flushed and cleared for table '%s'.", table.getName()));
    }

    /** Load a page into cache, evicting LRU if necessary. */
    public IndexPage loadPage(int pageID) {
        IndexPage newPage = new IndexPage(pageID, table, columnIndex);
        try {
            byte[] pageBuffer = fileIO.readPage(table.getIndexPath(columnIndex), newPage.getPagePos(),newPage.sizeInBytes());
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

    public synchronized IndexPage get(int pageID) {
        IndexPage page = cache.get(pageID);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format("Cache miss for page ID %d in table '%s'. Loading...", pageID, table.getName()));
            return loadPage(pageID);
        }
    }

    public synchronized IndexPage getLast(){
        int lastPageId = table.getIndexManager().getPages(columnIndex) - 1;
        if (lastPageId == -1) lastPageId = 0;
        return this.get(lastPageId);
    }

    public synchronized void put(IndexPage page) {
        cache.put(page.getPageID(), page);
    }

    public synchronized IndexPage remove(int pageID) {
        return cache.remove(pageID);
    }

    public synchronized boolean containsKey(int pageID) {
        return cache.containsKey(pageID);
    }

    public void deleteLastPage(IndexPage page) {
        this.cache.remove(page.getPageID());
        FileIO fileIO = new FileIO(table.getFileIOThread());
        try {
            fileIO.truncateFile(table.getIndexPath(columnIndex), page.sizeInBytes());
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing last page.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing last page.", e);
            Thread.currentThread().interrupt(); // good practice to reset interrupt status
        }
        table.getIndexManager().removeOnePage(columnIndex);
    }
}