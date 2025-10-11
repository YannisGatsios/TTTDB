package com.database.db.core.cache;

import java.util.Map;
import java.util.logging.Logger;

import com.database.db.core.Database;
import com.database.db.core.page.IndexPage;
import com.database.db.core.page.Page;
import com.database.db.core.page.TablePage;
import com.database.db.core.table.Table;

public class TransactionCache extends Cache{
    private static final Logger logger = Logger.getLogger(TransactionCache.class.getName());

    private final Cache parent;
    private final Database database;
    public TransactionCache(Database database, Cache parentCache, String name){
        super(database, -1);
        this.name = name;
        this.parent = parentCache;
        this.database = database;
    }

    @Override
    public void commit() {
        logger.info(name + ": Starting transaction commit...");

        // Merge this transaction cache into the parent cache
        this.cache.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> {
                logger.fine(name + ": Merging page '" + entry.getKey() + "' into parent cache");
                parent.put(entry.getKey(), entry.getValue());
            });

        // Clear this transaction's cache
        cache.clear();
        logger.info(name + ": Transaction cache cleared.");

        // If the parent is another transaction cache, do not commit tables yet
        if (parent instanceof TransactionCache) {
            logger.info(name + ": Parent is a TransactionCache, skipping table/index commit.");
            return;
        }

        // Commit all tables and their indexes to the database
        for (Table table : database.getAllTablesList()) {
            logger.info(name + ": Committing table: " + table.getName());
            table.commit();
            table.getIndexManager().commit();
            logger.info(name + ": Committed indexes for table: " + table.getName());
        }

        logger.info(name + ": Transaction commit completed.");
    }
    @Override
    protected void writePage(Map.Entry<String, Page> eldest){
        throw new UnsupportedOperationException("Can not use writeCache in transaction cache: Transaction "+name);
    }
    @Override
    protected TablePage loadTablePage(String pageKey){
        TablePage result = parent.getTablePage(pageKey);
        if(result.isDirty()) {
            result = result.deepCopy();
            this.cache.put(pageKey, result);
            return result;
        }
        parent.remove(pageKey);
        this.cache.put(pageKey, result);
        return result;
    }
    @Override
    protected IndexPage loadIndexPage(String pageKey){
        IndexPage result = parent.getIndexPage(pageKey);
        if(result.isDirty()) {
            result = result.deepCopy();
            this.cache.put(pageKey, result);
            return result;
        }
        parent.remove(pageKey);
        this.cache.put(pageKey, result);
        return result;
    }
    public String getName() { return this.name; }
    public Cache getParent() { return this.parent; }
}