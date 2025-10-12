package com.database.tttdb.core.cache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.tttdb.core.Database;
import com.database.tttdb.core.FileIO;
import com.database.tttdb.core.FileIOThread;
import com.database.tttdb.core.manager.IndexManager;
import com.database.tttdb.core.page.IndexPage;
import com.database.tttdb.core.page.Page;
import com.database.tttdb.core.page.TablePage;
import com.database.tttdb.core.table.Table;

public class Cache {
    private static final Logger logger = Logger.getLogger(Cache.class.getName());

    private final FileIO fileIO;
    private final Database database;
    protected String name = "Main Cache";


    // LRU cache using LinkedHashMap
    protected final Map<PageKey, Page> cache;

    private final int CAPACITY;
    private final int DELETION_CAPACITY;

    public Cache(Database database, int capacity){
        this.database = database;
        this.fileIO = new FileIO(database.getFileIOThread());
        this.CAPACITY = capacity;
        this.DELETION_CAPACITY = CAPACITY/10;
        if(capacity == -1) this.cache = new HashMap<>();
        else{
            this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<PageKey, Page> eldest) {
                    if (size() > capacity) {
                        writePage(eldest);
                        return true;
                    }
                    return false;
                }
            };
        }
    }

    public void commit(){
        logger.info(name + ": Starting commit operation...");
        cache.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                try {
                    writePage(entry);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, name+": Failed to write page: " + entry.getKey(), e);
                }
            });
        // Truncate tables and indexes
        for (Table table : database.getAllTablesList()) {
            logger.info(name + ": Truncating table: " + table.getName());
            truncateTable(table);
            for (int i = 0; i < table.getSchema().getNumOfColumns(); i++) {
                if (table.getIndexManager().isIndexed(i)) {
                    truncateIndex(table, i);
                }
            }
        }
        // Clear cache aft
        cache.clear();
        logger.info(name + ": Commit operation completed. Cache cleared.");
    }
    public void rollback(){
        logger.info(name + ": Starting rollback operation...");
        cache.clear();
        logger.info(name + ": Cache cleared.");
        for (Table table : database.getAllTablesList()) {
            logger.info(name + ": Rolling back table: " + table.getName());
            table.rollback();
            table.getIndexManager().rollback();
            logger.info(name + ": Rolled back indexes for table: " + table.getName());
        }
        logger.info(name + ": Rollback operation completed.");
    }

    //== Writing Pages ==
    protected void writePage(Map.Entry<PageKey, Page> eldest){
        Page page = eldest.getValue();
        fileIO.writePage(page.getFilePath(), page.toBytes(), page.getPagePos());
    }

    //== Loading Pages ==
    protected TablePage loadTablePage(PageKey pageKey) {
        Table table = database.getTable(pageKey.getTableName());
        int pageID = pageKey.getPageId();
        TablePage newPage = new TablePage(pageID, table);
        if(table.getDeletedPagesSet().contains(pageKey)){
            table.getDeletedPagesSet().remove(pageKey);
            cache.put(pageKey, newPage);
            if(table.getDeletedPages() > 0)table.removeDeletedPage(pageKey);
            return newPage;
        }
        try {
            byte[] pageBuffer = fileIO.readPage(newPage.getFilePath(), newPage.getPagePos(),newPage.sizeInBytes());
            if(pageBuffer != null) newPage.fromBytes(pageBuffer);
            cache.put(pageKey, newPage);
            return newPage;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, String.format(name+": Interrupted while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, String.format(name+": Execution failed while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        }
        return null;
    }
    protected IndexPage loadIndexPage(PageKey pageKey) {
        Table table = database.getTable(pageKey.getTableName());
        int columnIndex = table.getSchema().getColumnIndex(pageKey.getColumnName());
        int pageID = pageKey.getPageId();
        IndexPage newPage = new IndexPage(pageID, table, columnIndex);
        IndexManager indexManager = table.getIndexManager();
        if(indexManager.getDeletedPagesSet(columnIndex).contains(pageKey)){
            indexManager.getDeletedPagesSet(columnIndex).remove(pageKey);
            cache.put(pageKey, newPage);
            if(indexManager.getDeletedPages(columnIndex) > 0)indexManager.removeOneDeleted(pageKey, columnIndex);
            return newPage;
        }
        try {
            byte[] pageBuffer = fileIO.readPage(newPage.getFilePath(), newPage.getPagePos(),newPage.sizeInBytes());
            if(pageBuffer != null) newPage.fromBytes(pageBuffer);
            cache.put(pageKey, newPage);
            return newPage;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, String.format(name+": Interrupted while loading page ID %d for index '%s'.", pageID, pageKey.getColumnName()), e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, String.format(name+": Execution failed while loading page ID %d for index '%s'.", pageID, pageKey.getColumnName()), e);
        }
        return null;
    }

    //== Getting Pages ==
    public synchronized TablePage getTablePage(PageKey pageKey) {
        TablePage page = (TablePage)cache.get(pageKey);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format(name+": Cache miss for Table page ID %s. Loading...", pageKey));
            return loadTablePage(pageKey);
        }
    }
    public synchronized IndexPage getIndexPage(PageKey pageKey) {
        IndexPage page = (IndexPage)cache.get(pageKey);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format(name+": Cache miss for Index page ID %s. Loading...", pageKey));
            return loadIndexPage(pageKey);
        }
    }

    //== Getting Lasts Pages ==
    public synchronized TablePage getLastTablePage(Table table){
        int lastPageId = table.getPages() - 1;
        if (lastPageId == -1) lastPageId = 0;
        PageKey pageKey = PageKey.table(table.getName(), lastPageId);
        return this.getTablePage(pageKey);
    }
    public synchronized IndexPage getLastIndexPage(Table table,int columnIndex){
        int lastPageId = table.getIndexManager().getPages(columnIndex) - 1;
        if (lastPageId == -1) lastPageId = 0;
        PageKey pageKey = PageKey.index(table.getName(), table.getSchema().getNames()[columnIndex], lastPageId);
        return this.getIndexPage(pageKey);
    }

    public synchronized void put(PageKey pageKey, Page page) {
        cache.put(pageKey, page);
    }
    public synchronized Page remove(PageKey pageKey) {
        return cache.remove(pageKey);
    }
    public void deleteLastTablePage(PageKey pageKey, Table table, TablePage page) {
        this.cache.remove(pageKey);
        table.getDeletedPagesSet().add(pageKey);
        table.removeOnePage();
        table.addDeletedPage(pageKey);
        if(CAPACITY == -1)return;
        if(table.getDeletedPages() >= DELETION_CAPACITY) truncateTable(table);
    }
    private void truncateTable(Table table){
        if (table.getDeletedPages() == 0) return;
        try {
            fileIO.truncateFile(table.getPath(), table.getDeletedPages()*Page.pageSizeInBytes(TablePage.sizeOfEntry(table)));
            table.clearDeletedPages();
            table.getDeletedPagesSet().clear();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, name+": ExecutionException while truncating file for removing "+table.getDeletedPages()+" last table pages.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, name+": InterruptedException while truncating file for removing "+table.getDeletedPages()+" last table pages.", e);
            Thread.currentThread().interrupt();
        }
    }
    public void deleteLastIndexPage(PageKey pageKey, Table table, IndexPage page) {
        this.cache.remove(pageKey);
        int columnIndex = page.getColumnIndex();
        IndexManager indexManager = table.getIndexManager();
        indexManager.getDeletedPagesSet(columnIndex).add(pageKey);
        indexManager.removeOnePage(columnIndex);
        indexManager.addOneDeletedPage(pageKey, columnIndex);
        if(CAPACITY == -1)return;
        if(indexManager.getDeletedPages(columnIndex) >= DELETION_CAPACITY) truncateIndex(table,columnIndex);
    }
    private void truncateIndex(Table table, int columnIndex){
        IndexManager indexManager = table.getIndexManager();
        if (indexManager.getDeletedPages(columnIndex) == 0) return;
        try {
            fileIO.truncateFile(table.getIndexPath(columnIndex), indexManager.getDeletedPages(columnIndex)*Page.pageSizeInBytes(IndexPage.sizeOfEntry(table, columnIndex)));
            indexManager.clearDeletedPages(columnIndex);
            indexManager.getDeletedPagesSet(columnIndex).clear();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, name+": ExecutionException while truncating file for removing "+indexManager.getDeletedPages(columnIndex)+" last index pages.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, name+": InterruptedException while truncating file for removing "+indexManager.getDeletedPages(columnIndex)+" last index page.", e);
            Thread.currentThread().interrupt();
        }
    }

    public Database getDatabase() { return this.database; }
    public int getCacheCapacity() { return this.CAPACITY; }
    public void setFileIOThread(FileIOThread fileIOThread) { this.fileIO.setFileIOThread(fileIOThread); }
}