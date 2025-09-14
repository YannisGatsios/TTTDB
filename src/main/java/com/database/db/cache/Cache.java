package com.database.db.cache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.Database;
import com.database.db.FileIO;
import com.database.db.manager.IndexManager;
import com.database.db.page.IndexPage;
import com.database.db.page.Page;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class Cache {
    private static final Logger logger = Logger.getLogger(Cache.class.getName());

    private final FileIO fileIO;
    private final Database database;


    // LRU cache using LinkedHashMap
    protected final Map<String, Page> cache;

    private final int CAPACITY;
    private final int DELETION_CAPACITY;

    public Cache(Database database, int capacity){
        this.database = database;
        this.fileIO = new FileIO();
        this.CAPACITY = capacity;
        this.DELETION_CAPACITY = CAPACITY/10;
        if(capacity == -1) this.cache = new HashMap<>();
        else{
            this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Page> eldest) {
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
        // Write all pages in alphabetical order
        cache.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                try {
                    writePage(entry);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to write page: " + entry.getKey(), e);
                }
            });
        // Truncate tables and indexes
        for (Table table : database.getAllTablesList()) {
            truncateTable(table);
            for (int i = 0; i < table.getSchema().getNumOfColumns(); i++) {
                if (table.getIndexManager().isIndexed(i)) {
                    truncateIndex(table, i);
                }
            }
        }
        // Clear cache aft
        cache.clear();
    }
    public void rollback(){
        cache.clear();
        for (Table table : database.getAllTablesList()) {
            table.rollBackDeletedPages();
            table.getIndexManager().rollBackDeletedPages();
        }
    }

    //== Writing Pages ==
    protected void writePage(Map.Entry<String, Page> eldest){
        String key = eldest.getKey();
        String[] ketParts = key.split("\\.");
        Table table = database.getTable(ketParts[0]);
        Page page = eldest.getValue();
        fileIO.setFileIOThread(table.getFileIOThread());
        fileIO.writePage(page.getFilePath(), page.toBytes(), page.getPagePos());
    }

    //== Loading Pages ==
    protected TablePage loadTablePage(String pageKey) {
        String[] keyParts = pageKey.split("\\.");
        Table table = database.getTable(keyParts[0]);
        int pageID = Integer.parseInt(keyParts[1]);
        TablePage newPage = new TablePage(pageID, table);
        if(table.getDeletedPagesSet().contains(pageKey)){
            table.getDeletedPagesSet().remove(pageKey);
            cache.put(pageKey, newPage);
            if(table.getDeletedPages() > 0)table.removeOneDeletedPage();
            return newPage;
        }
        try {
            fileIO.setFileIOThread(table.getFileIOThread());
            byte[] pageBuffer = fileIO.readPage(newPage.getFilePath(), newPage.getPagePos(),newPage.sizeInBytes());
            if(pageBuffer != null) newPage.fromBytes(pageBuffer);
            cache.put(pageKey, newPage);
            return newPage;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, String.format("Interrupted while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, String.format("Execution failed while loading page ID %d for table '%s'.", pageID, table.getName()), e);
        }
        return null;
    }
    protected IndexPage loadIndexPage(String pageKey) {
        String[] keyParts = pageKey.split("\\.");
        Table table = database.getTable(keyParts[0]);
        int columnIndex = table.getSchema().getColumnIndex(keyParts[1]);
        int pageID = Integer.parseInt(keyParts[2]);
        IndexPage newPage = new IndexPage(pageID, table, columnIndex);
        IndexManager indexManager = table.getIndexManager();
        if(indexManager.getDeletedPagesSet(columnIndex).contains(pageKey)){
            indexManager.getDeletedPagesSet(columnIndex).remove(pageKey);
            cache.put(pageKey, newPage);
            if(table.getIndexManager().getDeletedPages(columnIndex) > 0)table.getIndexManager().removeOneDeleted(columnIndex);;
            return newPage;
        }
        try {
            fileIO.setFileIOThread(table.getFileIOThread());
            byte[] pageBuffer = fileIO.readPage(newPage.getFilePath(), newPage.getPagePos(),newPage.sizeInBytes());
            if(pageBuffer != null) newPage.fromBytes(pageBuffer);
            cache.put(pageKey, newPage);
            return newPage;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.SEVERE, String.format("Interrupted while loading page ID %d for index '%s'.", pageID, keyParts[1]), e);
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, String.format("Execution failed while loading page ID %d for index '%s'.", pageID, keyParts[1]), e);
        }
        return null;
    }

    //== Getting Pages ==
    public synchronized TablePage getTablePage(String pageKey) {
        TablePage page = (TablePage)cache.get(pageKey);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format("Cache miss for Table page ID %s. Loading...", pageKey));
            return loadTablePage(pageKey);
        }
    }
    public synchronized IndexPage getIndexPage(String pageKey) {
        IndexPage page = (IndexPage)cache.get(pageKey);
        if (page != null) {
            return page;
        } else {
            logger.fine(String.format("Cache miss for Index page ID %s. Loading...", pageKey));
            return loadIndexPage(pageKey);
        }
    }

    //== Getting Lasts Pages ==
    public synchronized TablePage getLastTablePage(Table table){
        int lastPageId = table.getPages() - 1;
        if (lastPageId == -1) lastPageId = 0;
        String pageKey = table.getName()+"."+lastPageId;
        return this.getTablePage(pageKey);
    }
    public synchronized IndexPage getLastIndexPage(Table table,int columnIndex){
        int lastPageId = table.getIndexManager().getPages(columnIndex) - 1;
        if (lastPageId == -1) lastPageId = 0;
        String pageKey = table.getName()+"."+table.getSchema().getNames()[columnIndex]+"."+lastPageId;
        return this.getIndexPage(pageKey);
    }

    public synchronized void put(String pageKey, Page page) {
        cache.put(pageKey, page);
    }
    public synchronized Page remove(String pageKey) {
        return cache.remove(pageKey);
    }
    public void deleteLastTablePage(String pageKey, Table table, TablePage page) {
        this.cache.remove(pageKey);
        table.getDeletedPagesSet().add(pageKey);
        table.removeOnePage();
        table.addOneDeletedPage();
        if(CAPACITY == -1)return;
        if(table.getDeletedPages() >= DELETION_CAPACITY) truncateTable(table);
    }
    private void truncateTable(Table table){
        if (table.getDeletedPages() == 0) return;
        try {
            fileIO.setFileIOThread(table.getFileIOThread());
            fileIO.truncateFile(table.getPath(), table.getDeletedPages()*Page.pageSizeInBytes(TablePage.sizeOfEntry(table)));
            table.setDeletedPages(0);
            table.getDeletedPagesSet().clear();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing "+table.getDeletedPages()+" last table pages.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing "+table.getDeletedPages()+" last table pages.", e);
            Thread.currentThread().interrupt();
        }
    }
    public void deleteLastIndexPage(String pageKey, Table table, IndexPage page) {
        this.cache.remove(pageKey);
        int columnIndex = page.getColumnIndex();
        IndexManager indexManager = table.getIndexManager();
        indexManager.getDeletedPagesSet(columnIndex).add(pageKey);
        indexManager.removeOnePage(columnIndex);
        indexManager.addOneDeletedPage(columnIndex);
        if(CAPACITY == -1)return;
        if(indexManager.getDeletedPages(columnIndex) >= DELETION_CAPACITY) truncateIndex(table,columnIndex);
    }
    private void truncateIndex(Table table, int columnIndex){
        IndexManager indexManager = table.getIndexManager();
        if (indexManager.getDeletedPages(columnIndex) == 0) return;
        try {
            fileIO.setFileIOThread(table.getFileIOThread());
            fileIO.truncateFile(table.getIndexPath(columnIndex), indexManager.getDeletedPages(columnIndex)*Page.pageSizeInBytes(IndexPage.sizeOfEntry(table, columnIndex)));
            indexManager.setDeletedPage(columnIndex, 0);
            indexManager.getDeletedPagesSet(columnIndex).clear();
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing "+indexManager.getDeletedPages(columnIndex)+" last index pages.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing "+indexManager.getDeletedPages(columnIndex)+" last index page.", e);
            Thread.currentThread().interrupt();
        }
    }

    public Database getDatabase() { return this.database; }
    public int getCacheCapacity() { return this.CAPACITY; }
}