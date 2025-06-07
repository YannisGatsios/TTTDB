package com.database.db.page;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class PageCache<K extends Comparable<K>> {
    private final FileIO fileIO;

    private final int maxSize;
    private Table<K> table;
    private final Map<Integer,TablePage<K>> cache;

    public PageCache(Table<K> table){
        fileIO = new FileIO(table.getFileIOThread());
        this.maxSize = table.MAX_NUM_OF_PAGES_IN_CACHE;
        this.table = table;
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, TablePage<K>> eldest) {
                return size() > PageCache.this.maxSize;
            }
        };
    }
    public void writeCache() throws IOException{
        for (Map.Entry<Integer,TablePage<K>> entry : this.cache.entrySet()) {
            TablePage<K> page = entry.getValue();
            fileIO.writePage(table.getTablePath(), page.toBuffer(), page.getPagePos());
            this.cache.remove(page.getPageID());
        }
        fileIO.writeTree(table.getIndexPath(), this.table.getPrimaryKeyIndex().treeToBuffer(this.table.getPrimaryKeyMaxSize()));
    }
    public void loadPage(int pageID, Table<K> table) throws IOException,InterruptedException, ExecutionException  {
        if(this.cache.size() != this.maxSize){
            TablePage<K> newPage = new TablePage<>(pageID, table);
            newPage.bufferToPage(fileIO.readPage(table.getTablePath(), newPage.getPagePos(), newPage.sizeInBytes()), table);
            this.cache.put(pageID, newPage);
        }else{
            this.writeCache();
            TablePage<K> newPage = new TablePage<>(pageID, table);
            newPage.bufferToPage(fileIO.readPage(table.getTablePath(), newPage.getPagePos(), newPage.sizeInBytes()), table);
            this.cache.put(pageID, newPage);
        }
    }

    public synchronized TablePage<K> get(int pageID){
        return cache.get(pageID);
    }

    public synchronized void put(int pageID, TablePage<K> page){
        this.cache.put(pageID, page);
    }

    public synchronized TablePage<K> remove(int pageID){
        return this.cache.remove(pageID);
    }

    public synchronized boolean containsKey(int pageID){
        return this.cache.containsKey(pageID);
    }

    public synchronized void clear(){
        this.cache.clear();
    }
}
