package com.database.tttdb.core.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.database.tttdb.api.Condition.*;
import com.database.tttdb.api.DBMS.TableConfig;
import com.database.tttdb.api.Query.SelectType;
import com.database.tttdb.api.Row;
import com.database.tttdb.api.UpdateFields;
import com.database.tttdb.core.Database;
import com.database.tttdb.core.FileIO;
import com.database.tttdb.core.Database.TableReference;
import com.database.tttdb.core.cache.PageKey;
import com.database.tttdb.core.cache.TableCache;
import com.database.tttdb.core.cache.TableSnapshot;
import com.database.tttdb.core.index.Pair;
import com.database.tttdb.core.index.IndexInit.BlockPointer;
import com.database.tttdb.core.index.IndexInit.PointerPair;
import com.database.tttdb.core.manager.EntryManager;
import com.database.tttdb.core.manager.IndexManager;
import com.database.tttdb.core.manager.IndexManager.IndexRecord;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.page.TablePage;

public class Table {
    private final Database database;
    private final String tableName;
    private String path = "";
    private final TableSchema schema;
    
    private TableCache cache;
    private IndexManager indexManager;
    private AutoIncrementing[] autoIncrementing;
    private TableSnapshot tableSnapshot;

    private TableReference parent;
    private final List<TableReference> children = new ArrayList<>();

    private final String tableFilePath;
    private final String indexPathPrefix;


    public Table(Database database, TableConfig config) {
        this.database = database;
        this.path = this.database.getPath();
        this.tableName = config.tableName();
        this.schema = new TableSchema(tableName, config.schema().get(database));
        
        this.cache = new TableCache(this, database);
        this.indexManager = new IndexManager(this);
        this.tableSnapshot = new TableSnapshot();

        this.tableFilePath = path + database.getName() + "." + tableName + ".table";
        this.indexPathPrefix = path + database.getName() + "." + tableName + ".";
    }
    public void start(){
        int numOfPages = FileIO.getNumOfPages(this.getPath(),TablePage.sizeOfEntry(this));
        this.tableSnapshot.setNumOfPages(numOfPages);
        this.indexManager.initialize();
        this.autoIncrementing = AutoIncrementing.prepareAutoIncrementing(this);
    }

    public AutoIncrementing getAutoIncrementing(int columnIndex){
        if (columnIndex < 0 || columnIndex >= autoIncrementing.length) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        AutoIncrementing result = autoIncrementing[columnIndex];
        if (result == null) {
            throw new IllegalStateException("Column " + columnIndex + " is not auto-incrementing.");
        }
        return result;
    }
    public long nextAutoIncrementValue(int columnIndex){
        return this.getAutoIncrementing(columnIndex).getNextKey();
    }
    public long getAutoIncrementValue(int columnIndex){
        return this.getAutoIncrementing(columnIndex).getKey();
    }
    public void setAutoIncrementValue(int columnIndex, long value){
        this.getAutoIncrementing(columnIndex).setNextKey(value);;
    }

    // -- Transaction Management -- 
    public void beginTransaction(){
        this.tableSnapshot.beginTransaction();
        this.indexManager.beginTransaction();
    }
    public void commit() { 
        this.tableSnapshot.commit();
    }
    public void rollback() { 
        this.tableSnapshot.rollback();
    }

    // -- Entry Management -- 
    public List<Entry> select(WhereClause whereClause, int begin, int limit, SelectType type){
        return EntryManager.selectEntries(this, whereClause, begin, limit, type);
    }
    public int insert(List<Row> rows){
        return EntryManager.insertEntries(this, rows);
    }
    public void insertUnsafe(Entry entry){
        EntryManager.insertEntry(this, entry);
    }
    public int delete(WhereClause clause, int limit){
        return EntryManager.deleteEntry(this, clause, limit);
    }
    public int update(WhereClause whereClause, int limit, UpdateFields updates){
        return EntryManager.updateEntry(this, whereClause, limit, updates);
    }

    // -- Index Management -- 
    public <K extends Comparable<? super K>> List<IndexRecord<K>> selectIndex(WhereClause whereClause) {
        return this.indexManager.findRangeIndex(whereClause);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean containsKey(Object key, int columnIndex){
        return this.indexManager.isKeyFound((K)key, columnIndex);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<Pair<K, PointerPair>> searchIndex(Object key, int columnIndex){
        return this.indexManager.findBlock((K)key, columnIndex);
    }
    public void insertIndex(Entry entry, BlockPointer blockPointer){
        this.indexManager.insertIndex(entry, blockPointer);
    }
    public void removeIndex(Entry entry, BlockPointer blockPointer){
        this.indexManager.removeIndex(entry, blockPointer);
    }
    public void updateIndex(Entry entry, BlockPointer newValue, BlockPointer oldValue){
        this.indexManager.updateIndex(entry, newValue, oldValue);
    }

    // -- Snapshot Management -- 
    public int getPages() { return this.tableSnapshot.getNumOfPages(); }
    public void addOnePage() { this.tableSnapshot.addOnePage(); }
    public void removeOnePage() { this.tableSnapshot.removeOnePage(); }

    public Set<PageKey> getDeletedPagesSet() { return this.tableSnapshot.getDeletedPageIDSet(); }
    public int getDeletedPages() { return this.tableSnapshot.getDeletedPages(); }
    public void clearDeletedPages() { this.tableSnapshot.clearDeletedPages(); }
    public void addDeletedPage(PageKey pageKey) { this.tableSnapshot.addDeletedPage(pageKey); }
    public void removeDeletedPage(PageKey pageKey) { this.tableSnapshot.removeDeletedPage(pageKey); }

    // Getters/Setters
    public String getDatabaseName() { return database.getName(); }
    public Database getDatabase() { return this.database; }
    public String getName() { return this.tableName; }
    public TableSchema getSchema() { return this.schema; }
    public TableCache getCache() { return this.cache; }
    public IndexManager getIndexManager() { return this.indexManager; }

    // Foreign Key references 
    public void addParent(TableReference parent) { this.parent = parent; }
    public TableReference getParent() { return this.parent; }
    public void addChild(TableReference child) { this.children.add(child); }
    public List<TableReference> getChildren() { return this.children; }

    // Get Index and Table file paths for this Table. 
    public String getPath() { return this.tableFilePath; }
    public String getIndexPath(int columnIndex) { return this.indexPathPrefix + schema.getNames()[columnIndex] + ".index"; }
}