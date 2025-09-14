package com.database.db.table;

import java.io.File;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.database.db.Database;
import com.database.db.Database.TableReference;
import com.database.db.FileIOThread;
import com.database.db.api.Condition;
import com.database.db.api.Condition.*;
import com.database.db.api.DBMS.TableConfig;
import com.database.db.cache.TableCache;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.index.BTreeSerialization.PointerPair;
import com.database.db.index.Pair;
import com.database.db.manager.IndexManager;
import com.database.db.page.Entry;
import com.database.db.page.Page;
import com.database.db.page.TablePage;

public class Table {
    private final Database database;
    private final String tableName;
    private String path = "";
    private final SchemaInner schema;
    private TableCache cache;
    private IndexManager indexManager;
    private AutoIncrementing[] autoIncrementing;
    private FileIOThread fileIOThread;
    private Set<String> deletedPages = new HashSet<>();
    private Set<String> tmpDeletedPages = new HashSet<>();

    private TableReference parent;
    private List<TableReference> children = new ArrayList<>();
    
    private int numberOfPages;
    private int numOfDeletedPages = 0;
    private int tmpNumOfDeletedPages = 0;

    public Table(Database database, TableConfig config) {
        this.database = database;
        this.path = this.database.getPath();
        this.tableName = config.tableName();
        this.schema = new SchemaInner(config.schema().get(database));
        this.cache = new TableCache(this, database);
        this.indexManager = new IndexManager(this);
    }
    public void start(){
        this.fileIOThread = new FileIOThread(this.tableName);
        this.fileIOThread.start();
        this.numberOfPages = Table.getNumOfPages(this.getPath(),TablePage.sizeOfEntry(this));
        this.indexManager.initialize();
        this.autoIncrementing = this.getAutoIncrementing();
    }
    public void close() throws InterruptedException{
        this.fileIOThread.shutdown();
        this.fileIOThread = null;
    }
    public static int getNumOfPages(String path, int sizeOfEntry){
        File file = new File(path);
        long fileSize = file.length();
        int pageSize = Page.pageSizeInBytes(sizeOfEntry);
        return (int) ((fileSize + pageSize - 1) / pageSize);
    }

    private AutoIncrementing[] getAutoIncrementing(){
        AutoIncrementing[] result = new AutoIncrementing[this.schema.getNumOfColumns()];
        boolean[] isAutoIncrementing = this.schema.getAutoIncrementIndex();
        for (int i = 0;i<result.length;i++){
            if(isAutoIncrementing[i]){
                long max;
                Object maxValue = this.indexManager.getMax(i);
                if(this.indexManager.isIndexed(i) && maxValue != null) max = (long)maxValue;
                else if (this.indexManager.isIndexed(i) && maxValue == null) max = 0;
                else{
                    max = this.getMaxSequential(i);
                }
                result[i] = new AutoIncrementing(max+1);
            }
        }
        return result;
    }
    private long getMaxSequential(int columnIndex){
        long max = 0;
        for (int i = 0;i<this.numberOfPages;i++){
            TablePage page = this.cache.getTablePage(i);
            Entry[] entries = page.getAll();
            for (Entry entry : entries) {
                if(max < (long)entry.get(columnIndex)) max = (long)entry.get(columnIndex);
            }
        }
        return max;
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

    public record IndexRecord<K>(K key, PointerPair value, int columnIndex) {
            @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IndexRecord<?> other)) return false;
            return Objects.equals(value, other.value);
        }
        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<IndexRecord<K>> findRangeIndex(WhereClause whereClause) {
        if (whereClause == null) return this.noCondition();
        Object start = null;
        Object end   = null;
        List<Map.Entry<Clause, Condition<WhereClause>>> clauses = whereClause.getConditions();
        List<IndexRecord<K>> previousPairList = new ArrayList<>();
        for (Map.Entry<Clause, Condition<WhereClause>> conditionEntry : clauses) {
            Condition<WhereClause> condition = conditionEntry.getValue();
            EnumMap<Conditions, Object> conditionList = condition.getConditions();
            if (conditionList.containsKey(Conditions.IS_BIGGER)) 
                start = conditionList.get(Conditions.IS_BIGGER);
            if (conditionList.containsKey(Conditions.IS_SMALLER)) 
                end = conditionList.get(Conditions.IS_SMALLER);
            if (conditionList.containsKey(Conditions.IS_BIGGER_OR_EQUAL)) 
                start = conditionList.get(Conditions.IS_BIGGER_OR_EQUAL);
            if (conditionList.containsKey(Conditions.IS_SMALLER_OR_EQUAL)) 
                end = conditionList.get(Conditions.IS_SMALLER_OR_EQUAL);
            if (!conditionList.containsKey(Conditions.IS_BIGGER) &&
                !conditionList.containsKey(Conditions.IS_SMALLER) &&
                !conditionList.containsKey(Conditions.IS_BIGGER_OR_EQUAL) &&
                !conditionList.containsKey(Conditions.IS_SMALLER_OR_EQUAL) &&
                conditionList.containsKey(Conditions.IS_EQUAL)) {
                start = conditionList.get(Conditions.IS_EQUAL);
                end   = conditionList.get(Conditions.IS_EQUAL);
            }
            int columnIndex = this.schema.getColumnIndex(condition.getColumnName());
            List<Pair<K, PointerPair>> indexResult = this.indexManager.findRangeIndex((K) start, (K) end, columnIndex);
            List<IndexRecord<K>> resultInner = new ArrayList<>();
            for (Pair<K, PointerPair> pair : indexResult) {
                if (!condition.isApplicable(pair.key)) continue;
                resultInner.add(new IndexRecord<K>(pair.key, pair.value, columnIndex));
            }
            switch (conditionEntry.getKey()) {
                case FIRST:
                    previousPairList = resultInner;
                    break;
                case OR:
                    Set<IndexRecord<K>> orSet = new HashSet<>(previousPairList);
                    orSet.addAll(resultInner);
                    previousPairList = new ArrayList<>(orSet);
                    break;
                case AND:
                    previousPairList.retainAll(resultInner);
                    break;
                case FIRST_GROUP:
                case OR_GROUP:
                case AND_GROUP:
            }
        }
        return previousPairList;
    }
    private <K extends Comparable<? super K>> List<IndexRecord<K>> noCondition(){
        int columnIndex = this.schema.getPrimaryKeyIndex();
        if(columnIndex == -1){
            boolean[] unique = this.schema.getUniqueIndex();
            for (int i = 0;i<unique.length;i++) {
                if(unique[i]){
                    columnIndex = i;
                    break;
                }
            }
        }
        if(columnIndex == -1){
            boolean[] Index = this.schema.getIndexIndex();
            for (int i = 0;i<Index.length;i++) {
                if(Index[i]){
                    columnIndex = i;
                    break;
                }
            }
        }
        if(columnIndex == -1) columnIndex = 0;
        List<Pair<K, PointerPair>> indexResult = this.indexManager.findRangeIndex(null, null, columnIndex);
        List<IndexRecord<K>> result = new ArrayList<>();
        for (Pair<K, PointerPair> pair : indexResult) {
            result.add(new IndexRecord<K>(pair.key, pair.value, columnIndex));
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(Object key, int columnIndex){
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

    public String getDatabaseName() { return database.getName(); }
    public String getName() { return this.tableName; }
    public SchemaInner getSchema() { return this.schema; }
    public TableCache getCache() { return this.cache; }
    public FileIOThread getFileIOThread() { return this.fileIOThread; }
    public IndexManager getIndexManager() { return this.indexManager; }

    public void addParent(TableReference parent) { this.parent = parent; }
    public TableReference getParent() { return this.parent; }
    public void addChild(TableReference child) { this.children.add(child); }
    public List<TableReference> getChildren() { return this.children; }

    //Get Index and Table file paths for this Table. 
    public String getPath() { return this.path+database.getName()+"."+this.getName()+".table"; }
    public String getIndexPath(int columnIndex) { return this.path+database.getName()+"."+this.getName()+"."+this.schema.getNames()[columnIndex]+".index"; }

    public int getPages() { return this.numberOfPages; }
    public void addOnePage() { this.numberOfPages++; }
    public void removeOnePage() { this.numberOfPages--; }

    public Set<String> getDeletedPagesSet() { return this.tmpDeletedPages; }
    public int getDeletedPages() { return this.tmpNumOfDeletedPages; }
    public void setDeletedPages(int numOfDeletedPages) { this.tmpNumOfDeletedPages = numOfDeletedPages; }
    public void addOneDeletedPage() { this.tmpNumOfDeletedPages++; }
    public void removeOneDeletedPage() { this.tmpNumOfDeletedPages--; }
    public void commitDeletedPages() { 
        this.numOfDeletedPages = this.tmpNumOfDeletedPages; 
        this.deletedPages = new HashSet<>(this.tmpDeletedPages);
    }
    public void rollBackDeletedPages() { 
        this.tmpNumOfDeletedPages = this.numOfDeletedPages; 
        this.tmpDeletedPages = new HashSet<>(this.deletedPages);
    }
}
