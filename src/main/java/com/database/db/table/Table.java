package com.database.db.table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIOThread;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.DBMS.CacheCapacity;
import com.database.db.api.Condition.Clause;
import com.database.db.api.Condition.Conditions;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.index.BTreeSerialization.PointerPair;
import com.database.db.index.Pair;
import com.database.db.manager.IndexManager;
import com.database.db.page.Cache;
import com.database.db.page.Entry;
import com.database.db.page.Page;
import com.database.db.page.TablePage;

public class Table {
    private String databaseName;
    private String tableName;
    private Schema schema;
    private Cache cache;
    private Cache tmpCache;//Used for transatctions.
    private IndexManager indexes;

    private int numberOfPages;

    private AutoIncrementing[] autoIncrementing;

    public CacheCapacity cacheCapacity = new CacheCapacity(2,2);
    private String path = "";
    private FileIOThread fileIOThread;

    public Table(String databaseName, String tableName,String schemaConfig, FileIOThread fileIOThread, CacheCapacity cacheCapacity, String path) throws InterruptedException, ExecutionException, IOException {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schema = new Schema(schemaConfig.split(";"));
        this.path = path;
        this.fileIOThread = fileIOThread;
        if(cacheCapacity != null) this.cacheCapacity = cacheCapacity;
        this.cache = new Cache(this);
        this.numberOfPages = Table.getNumOfPages(this.getPath(),TablePage.sizeOfEntry(this));
        this.indexes = new IndexManager(this);
        this.autoIncrementing = this.getAutoIncrementing();
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

    public void startTransaction(){
        this.commit();
        if(this.tmpCache == null)this.tmpCache = this.cache;
        this.cacheCapacity = new CacheCapacity(-1,-1);
        this.cache = new Cache(this);
    }
    public void commit(){
        this.cache.clear();
        if(this.tmpCache != null){
            this.cache = this.tmpCache;
            this.tmpCache = null;
            this.cacheCapacity = this.cache.getCacheCapacity();
        }
    }
    
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<PointerPair> findRangeIndex(WhereClause whereClause){
        if(whereClause == null) return this.noCondition();
        Object start = null;
        Object end  = null;
        ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(this.schema.getNames()));
        List<Map.Entry<Clause, WhereClause>> clauses = whereClause.getClauseList();
        List<PointerPair> previousPointerList = new ArrayList<>();
        for (Map.Entry<Clause, WhereClause> condition : clauses) {
            WhereClause queryCondition = condition.getValue();
            EnumMap<Conditions, Object> conditionList = queryCondition.getConditionElementsList();
            if(conditionList.containsKey(Conditions.IS_BIGGER))
                start = conditionList.get(Conditions.IS_BIGGER);
            if(conditionList.containsKey(Conditions.IS_SMALLER))
                end = conditionList.get(Conditions.IS_SMALLER);
            if(!conditionList.containsKey(Conditions.IS_BIGGER) &&
            !conditionList.containsKey(Conditions.IS_SMALLER) &&
            conditionList.containsKey(Conditions.IS_EQUAL)){
                start = conditionList.get(Conditions.IS_EQUAL);
                end = conditionList.get(Conditions.IS_EQUAL);
            }
            int columnIndex = columnNames.indexOf(queryCondition.getColumnName());
            List<Pair<K,PointerPair>> indexResult = this.indexes.findRangeIndex((K)start, (K)end, columnIndex);
            List<PointerPair> resultInner = new ArrayList<>();
            for (Pair<K,PointerPair> pair : indexResult) {
                if(!queryCondition.isApplicable(pair.key)) continue;
                resultInner.add(pair.value);
            }
            switch (condition.getKey()) {
                case FIRST:
                    previousPointerList = resultInner;
                    break;
                case OR:
                    Set<PointerPair> orSet = new HashSet<>(previousPointerList);
                    orSet.addAll(resultInner);
                    previousPointerList = new ArrayList<>(orSet);
                    break;
                case AND:
                    previousPointerList.retainAll(resultInner);
                    break;
            }
        }
        return previousPointerList;
    }
    private <K extends Comparable<? super K>> List<PointerPair> noCondition(){
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
        List<Pair<K,PointerPair>> indexResult = this.indexes.findRangeIndex(null, null, columnIndex);
        List<PointerPair> result = new ArrayList<>();
        for (Pair<K,PointerPair> pair : indexResult) {
            result.add(pair.value);
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(Object key, int columnIndex){
        return this.indexes.isKeyFound((K)key, columnIndex);
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

    public String getDatabaseName(){return this.databaseName;}
    public String getName(){return this.tableName;}
    public Schema getSchema(){return this.schema;}
    public Cache getCache(){return this.cache;}
    public FileIOThread getFileIOThread() {return this.fileIOThread;}
    public IndexManager getIndexManager(){return this.indexes;}

    //Get Index and Table file paths for this Table. 
    public String getPath(){return this.path+this.getDatabaseName()+"."+this.getName()+".table";}
    public String getIndexPath(int columnIndex){return this.path+this.getDatabaseName()+"."+this.getName()+"."+this.schema.getNames()[columnIndex]+".index";}

    public int getPages(){return this.numberOfPages;}
    public void addOnePage(){this.numberOfPages++;}
    public void removeOnePage(){this.numberOfPages--;}
}
