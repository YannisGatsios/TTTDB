package com.database.db.page;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class Cache {

    private final Table table;
    private final FileIO fileIO;
    private final int capacity;

    public TableCache tableCache;
    public IndexCache[] indexCaches;

    public Cache(Table table) {
        this.table = table;
        this.fileIO = new FileIO(table.getFileIOThread());
        this.capacity = table.CACHE_CAPACITY;
        this.tableCache = new TableCache(table, table.CACHE_CAPACITY, fileIO);
        this.indexCaches = this.initializeIndexCaches();
    }

    private IndexCache[] initializeIndexCaches(){
        boolean[] indexedColumns = table.getSchema().isIndexed();
        IndexCache indexedCache[] = new IndexCache[indexedColumns.length];
        for (int i = 0;i<indexedCache.length;i++) {
            if(indexedColumns[i]) indexedCache[i] = new IndexCache(table, capacity, fileIO, i);
        }
        return indexedCache;
    }

    public synchronized void clear(){
        this.tableCache.writeCache();
        for (IndexCache indexCache : indexCaches) {
            if(indexCache!=null) indexCache.writeCache();
        }
    }
}