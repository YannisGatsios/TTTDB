package com.database.db.page;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.database.db.FileIO;
import com.database.db.bPlusTree.BPlusTree;
import com.database.db.table.Table;

public class PageCache<K extends Comparable<K>> {
    private final FileIO fileIO = new FileIO();

    private final int maxSize;
    private BPlusTree<K,?> tree;
    private final Map<Integer,Page<K>> cache;

    public PageCache(int maxSize, BPlusTree<K,?> tree){
        this.maxSize = maxSize;
        this.tree = tree;
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {

            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page<K>> eldest) {
                return size() > PageCache.this.maxSize;
            }
        };
    }

    public void writeCache(Table table) throws IOException{
        for (Map.Entry<Integer,Page<K>> entry : this.cache.entrySet()) {
            Page<K> page = entry.getValue();
            fileIO.writePage(table.getTablePath(), page.pageToBuffer(page), page.getPagePos());
            this.cache.remove(page.getPageID());
        }
        fileIO.writeTree(table.getIndexPath(), this.tree.treeToBuffer(this.tree, table.getMaxIDSize()));
    }

    public void loadPage(int pageID, Table table) throws IOException {
        if(this.cache.size() != this.maxSize){
            Page<K> newPage = new Page<>(pageID, table);
            newPage = newPage.bufferToPage(fileIO.readPage(table.getTablePath(), newPage.getPagePos(), newPage.sizeInBytes()), table);
            this.cache.put(pageID, newPage);
        }else{
            this.writeCache(table);
            Page<K> newPage = new Page<>(pageID, table);
            newPage = newPage.bufferToPage(fileIO.readPage(table.getTablePath(), newPage.getPagePos(), newPage.sizeInBytes()), table);
            this.cache.put(pageID, newPage);
        }
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
