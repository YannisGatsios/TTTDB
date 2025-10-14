package com.database.tttdb.core.cache;

import java.util.Map;
import java.util.logging.Logger;

import com.database.tttdb.core.Database;
import com.database.tttdb.core.page.IndexPage;
import com.database.tttdb.core.page.Page;
import com.database.tttdb.core.page.TablePage;
import com.database.tttdb.core.table.Table;

public class TransactionCache extends Cache{
    private static final Logger logger = Logger.getLogger(TransactionCache.class.getName());

    private final Cache parent;
    private final Database database;
    public TransactionCache(Database database, Cache parentCache, String name){
        super(database, -1);
        this.name = name + "Transaction";
        this.parent = parentCache;
        this.database = database;
    }

    @Override
    public void commit() {
        logger.info(name + ":\n == TRANSACTION COMMIT START ==");

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
            logger.info(name + ": Parent cache is not Main Cache, skipping table/index commit.\n"+
                                " == TRANSACTION COMMIT END ==");
            return;
        }

        // Commit all tables and their indexes to the database
        for (Table table : database.getAllTablesList()) {
            logger.info(name + ": Committing table: " + table.getName());
            table.commit();
            table.getIndexManager().commit();
            logger.info(name + ": Committed indexes for table: " + table.getName());
        }

        logger.info(name + ":\n == TRANSACTION COMMIT END ==");
    }
    @Override
    protected void writePage(Map.Entry<PageKey, Page> eldest){
        throw new UnsupportedOperationException("Can not use writeCache in transaction cache: Transaction "+name);
    }
    @Override
    protected TablePage loadTablePage(PageKey pageKey){
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
    protected IndexPage loadIndexPage(PageKey pageKey){
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