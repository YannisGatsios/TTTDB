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
import com.database.db.FileIOThread;
import com.database.db.api.Condition;
import com.database.db.api.Condition.*;
import com.database.db.api.DBMS.CacheCapacity;
import com.database.db.api.DBMS.TableConfig;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.index.BTreeSerialization.PointerPair;
import com.database.db.index.Pair;
import com.database.db.manager.IndexManager;
import com.database.db.page.Cache;
import com.database.db.page.Entry;
import com.database.db.page.Page;
import com.database.db.page.TablePage;

public class Table {
    private Database database;
    private String tableName;
    private SchemaInner schema;
    private Cache cache;
    private Cache tmpCache;//Used for transactions.
    private IndexManager indexes;

    private int numberOfPages;

    private AutoIncrementing[] autoIncrementing;

    public CacheCapacity cacheCapacity = new CacheCapacity(2,2);
    private String path = "";
    private FileIOThread fileIOThread;

    public Table(Database database, TableConfig config) {
        this.database = database;
        this.path = database.getPath();
        this.tableName = config.tableName();
        this.schema = new SchemaInner(config.schema().get(database));
        if(config.cacheCapacity() != null) this.cacheCapacity = config.cacheCapacity();
    }
    public void start(){
        this.fileIOThread = new FileIOThread();
        this.fileIOThread.start();
        this.cache = new Cache(this);
        this.numberOfPages = Table.getNumOfPages(this.getPath(),TablePage.sizeOfEntry(this));
        this.indexes = new IndexManager(this);
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
                Object maxValue = this.indexes.getMax(i);
                if(this.indexes.isIndexed(i) && maxValue != null) max = (long)maxValue;
                else if (this.indexes.isIndexed(i) && maxValue == null) max = 0;
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
            TablePage page = this.cache.tableCache.get(i);
            Entry[] entries = page.getAll();
            for (Entry entry : entries) {
                if(max < (long)entry.get(columnIndex)) max = (long)entry.get(columnIndex);
            }
        }
        return max;
    }

    public long nextAutoIncrementValue(int columnIndex){
        if (columnIndex < 0 || columnIndex >= autoIncrementing.length) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        AutoIncrementing result = autoIncrementing[columnIndex];
        if (result == null) {
            throw new IllegalStateException("Column " + columnIndex + " is not auto-incrementing.");
        }
        return result.getNextKey();
    }

    public long getAutoIncrementValue(int columnIndex){
        if (columnIndex < 0 || columnIndex >= autoIncrementing.length) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        AutoIncrementing result = autoIncrementing[columnIndex];
        if (result == null) {
            throw new IllegalStateException("Column " + columnIndex + " is not auto-incrementing.");
        }
        return result.getKey();
    }
    public void setAutoIncrementValue(int columnIndex, long value){
        if (columnIndex < 0 || columnIndex >= autoIncrementing.length) {
            throw new IllegalArgumentException("Invalid column index: " + columnIndex);
        }
        AutoIncrementing result = autoIncrementing[columnIndex];
        if (result == null) {
            throw new IllegalStateException("Column " + columnIndex + " is not auto-incrementing.");
        }
        result.setNextKey(value);;
    }

    public void startTransaction(){
        this.commit();
        if(this.tmpCache == null)this.tmpCache = this.cache;
        this.cacheCapacity = new CacheCapacity(-1,-1);
        this.cache = new Cache(this);
    }
    public void rollBack(){
        if(this.tmpCache != null){
            this.cache = this.tmpCache;
            this.tmpCache = null;
            this.cacheCapacity = this.cache.getCacheCapacity();
        }
        this.cache.rollBack();
    }
    public void commit(){
        this.cache.clear();
        if(this.tmpCache != null){
            this.cache = this.tmpCache;
            this.tmpCache = null;
            this.cacheCapacity = this.cache.getCacheCapacity();
        }
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
            List<Pair<K, PointerPair>> indexResult = this.indexes.findRangeIndex((K) start, (K) end, columnIndex);

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
        List<Pair<K, PointerPair>> indexResult = this.indexes.findRangeIndex(null, null, columnIndex);
        List<IndexRecord<K>> result = new ArrayList<>();
        for (Pair<K, PointerPair> pair : indexResult) {
            result.add(new IndexRecord<K>(pair.key, pair.value, columnIndex));
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(Object key, int columnIndex){
        return this.indexes.isKeyFound((K)key, columnIndex);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<PointerPair> searchIndex(Object key, int columnIndex){
        return this.indexes.findBlock((K)key, columnIndex);
    }
    public void insertIndex(Entry entry, BlockPointer blockPointer){
        this.indexes.insertIndex(entry, blockPointer);
    }
    public void removeIndex(Entry entry, BlockPointer blockPointer){
        this.indexes.removeIndex(entry, blockPointer);
    }
    public void updateIndex(Entry entry, BlockPointer newValue, BlockPointer oldValue){
        this.indexes.updateIndex(entry, newValue, oldValue);
    }

    public String getDatabaseName(){return database.getName();}
    public String getName(){return this.tableName;}
    public SchemaInner getSchema(){return this.schema;}
    public Cache getCache(){return this.cache;}
    public FileIOThread getFileIOThread() {return this.fileIOThread;}
    public IndexManager getIndexManager(){return this.indexes;}

    //Get Index and Table file paths for this Table. 
    public String getPath(){return this.path+database.getName()+"."+this.getName()+".table";}
    public String getIndexPath(int columnIndex){return this.path+database.getName()+"."+this.getName()+"."+this.schema.getNames()[columnIndex]+".index";}

    public int getPages(){return this.numberOfPages;}
    public void addOnePage(){this.numberOfPages++;}
    public void removeOnePage(){this.numberOfPages--;}
}
