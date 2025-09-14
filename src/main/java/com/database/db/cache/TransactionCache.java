package com.database.db.cache;

import java.util.Map;

import com.database.db.Database;
import com.database.db.page.IndexPage;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class TransactionCache extends Cache{
    private final String name;
    private final Cache parent;
    private final Database database;
    public TransactionCache(Database database, Cache parentCache, String name){
        super(database, -1);
        this.name = name;
        this.parent = parentCache;
        this.database = database;
    }

    @Override
    public void commit(){
        this.cache.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> parent.put(entry.getKey(), entry.getValue()));

        // Clear cache aft
        cache.clear();
        if(parent instanceof TransactionCache) return;
        // Truncate tables and indexes
        for (Table table : database.getAllTablesList()) {
            table.commitDeletedPages();
            table.getIndexManager().commitDeletedPages();
        }
    }
    @Override
    protected TablePage loadTablePage(String pageKey){
        return parent.getTablePage(pageKey);
    }
    @Override
    protected IndexPage loadIndexPage(String pageKey){
        return parent.getIndexPage(pageKey);
    }
    public String getName() { return this.name; }
    public Cache getParent() { return this.parent; }
}