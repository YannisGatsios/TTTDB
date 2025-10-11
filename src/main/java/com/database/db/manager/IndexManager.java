package com.database.db.manager;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.database.db.FileIO;
import com.database.db.api.Condition;
import com.database.db.api.Condition.Clause;
import com.database.db.api.Condition.Conditions;
import com.database.db.api.Condition.WhereClause;
import com.database.db.cache.IndexSnapshot;
import com.database.db.cache.TableSnapshot;
import com.database.db.cache.IndexSnapshot.Operation;
import com.database.db.cache.IndexSnapshot.OperationEnum;
import com.database.db.index.IndexInit;
import com.database.db.index.SecondaryKey;
import com.database.db.index.Pair;
import com.database.db.index.PrimaryKey;
import com.database.db.index.IndexInit.BlockPointer;
import com.database.db.index.IndexInit.PointerPair;
import com.database.db.page.Entry;
import com.database.db.page.IndexPage;
import com.database.db.table.SchemaInner;
import com.database.db.table.Table;
import com.database.db.table.DataType;
import com.database.db.index.Unique;

public class IndexManager {

    private final Table table;
    SchemaInner schema;
    private final IndexInit<?>[] indexes;
    public final IndexPageManager pageManager;
    private final TableSnapshot[] tableSnapshots;
    private final IndexSnapshot indexSnapshot;

    public IndexManager(Table table) {
        this.table = table;
        this.schema = table.getSchema();
        this.indexes = new IndexInit<?>[schema.getNumOfColumns()];

        this.pageManager = new IndexPageManager(table);
        int PrimaryKeyIndex = schema.getPrimaryKeyIndex();
        boolean[] unIndexes = schema.getUniqueIndex();
        boolean[] inIndexes = schema.getIndexIndex();
        for (int i = 0;i<this.indexes.length;i++) {
            if(PrimaryKeyIndex == i) this.indexes[i] = this.newPrimaryKey(i);
            else if (unIndexes[i]) this.indexes[i] = this.newUnique(i);
            else if (inIndexes[i]) this.indexes[i] = this.newIndex(i);
        }

        int cols = schema.getNumOfColumns();
        this.tableSnapshots = new TableSnapshot[cols];
        this.indexSnapshot = new IndexSnapshot();
        for (int i = 0; i < cols; i++) {
            if(isIndexed(i)){
                this.tableSnapshots[i] = new TableSnapshot();
            }
        }
    }
    public void initialize(){
        for (IndexInit<?> index : indexes) {
            if(index != null) index.initialize(table);
        }
        for (int i = 0;i<this.indexes.length;i++) {
            if(this.isIndexed(i)){
                int numOfPages =  FileIO.getNumOfPages(table.getIndexPath(i), IndexPage.sizeOfEntry(table, i));
                tableSnapshots[i].setNumOfPages(numOfPages);
            }
        }
    }

    private PrimaryKey<?> newPrimaryKey(int pkIndex) {
        DataType pkType = schema.getTypes()[pkIndex];
        return switch (pkType) {
            case INT -> new PrimaryKey<Integer>(table, pkIndex);
            case LONG -> new PrimaryKey<Long>(table, pkIndex);
            case FLOAT -> new PrimaryKey<Float>(table, pkIndex);
            case DOUBLE -> new PrimaryKey<Double>(table, pkIndex);
            case CHAR -> new PrimaryKey<String>(table, pkIndex);
            case DATE -> new PrimaryKey<Date>(table, pkIndex);
            case TIMESTAMP -> new PrimaryKey<Timestamp>(table, pkIndex);
            default -> throw new IllegalArgumentException("Unsupported primary key type: " + pkType);
        };
    }
    private Unique<?> newUnique(int uqIndex) {
        DataType type = schema.getTypes()[uqIndex];
        return switch (type) {
            case INT -> new Unique<Integer>(table, uqIndex);
            case LONG -> new Unique<Long>(table, uqIndex);
            case FLOAT -> new Unique<Float>(table, uqIndex);
            case DOUBLE -> new Unique<Double>(table, uqIndex);
            case CHAR -> new Unique<String>(table, uqIndex);
            case DATE -> new Unique<Date>(table, uqIndex);
            case TIMESTAMP -> new Unique<Timestamp>(table, uqIndex);
            default -> throw new IllegalArgumentException("Unsupported type: " + type.name());
        };
    }
    private SecondaryKey<?> newIndex(int skIndex) {
        DataType type = schema.getTypes()[skIndex];
        return switch (type) {
            case INT -> new SecondaryKey<Integer>(table, skIndex);
            case LONG -> new SecondaryKey<Long>(table, skIndex);
            case FLOAT -> new SecondaryKey<Float>(table, skIndex);
            case DOUBLE -> new SecondaryKey<Double>(table, skIndex);
            case CHAR -> new SecondaryKey<String>(table, skIndex);
            case DATE -> new SecondaryKey<Date>(table, skIndex);
            case TIMESTAMP -> new SecondaryKey<Timestamp>(table, skIndex);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    public boolean isIndexed(int columnIndex){
        if(this.indexes.length == 0) return false;
        return this.indexes[columnIndex] != null;
    }
    public Object getMax(int columnIndex){
        if(this.indexes[columnIndex] == null)return null;
        return this.indexes[columnIndex].getMax();
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
    public <K extends Comparable<? super K>> List<IndexRecord<K>> findRangeIndex(WhereClause whereClause){
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
            IndexInit<K> index = (IndexInit<K>)indexes[columnIndex];
            List<Pair<K, PointerPair>> indexResult = index == null?
                SequentialOperations.sequentialRangeSearch(table, (K)start, (K)end, columnIndex) :
                index.rangeSearch((K) start, (K) end);
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
    @SuppressWarnings("unchecked")
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
        IndexInit<K> index = (IndexInit<K>)indexes[columnIndex];
        List<Pair<K, PointerPair>> indexResult = index == null?
            SequentialOperations.sequentialRangeSearch(table, null, null, columnIndex) :
            index.rangeSearch(null, null);
        List<IndexRecord<K>> result = new ArrayList<>();
        for (Pair<K, PointerPair> pair : indexResult) {
            result.add(new IndexRecord<K>(pair.key, pair.value, columnIndex));
        }
        return result;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<Pair<K, PointerPair>> findBlock(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex);
        return ((IndexInit<K>)this.indexes[columnIndex]).search(key);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex).size()==0?false:true;
        return ((IndexInit<K>)this.indexes[columnIndex]).isKey(key);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void insertIndex(Entry entry, BlockPointer tablePointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        for (IndexInit<?> index : this.indexes) {
            if(index == null) continue;
            int columnIndex = index.getColumnIndex();
            K key = this.getValidatedKey(entry, index, columnIndex);
            BlockPointer indexPointer = this.pageManager.insert(tablePointer, key, index.getColumnIndex());
            PointerPair value = new PointerPair(tablePointer,indexPointer);
            ((IndexInit<K>) index).insert(key, value);
            keys[columnIndex] = key;
            values[columnIndex] = value;
        }
        Operation operation = new Operation(OperationEnum.INSERT, keys, values,null);
        indexSnapshot.addOperation(operation);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void removeIndex(Entry entry, BlockPointer blockPointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        Object[] updatedKeys = new Object[indexes.length];
        PointerPair[] updatedValues = new PointerPair[indexes.length];
        PointerPair[] updatedOldValues = new PointerPair[indexes.length];
        for (IndexInit<?> index : this.indexes) {
            if(index == null) continue;
            int columnIndex = index.getColumnIndex();
            K key = this.getValidatedKey(entry, index, columnIndex);
            BlockPointer indexPointer = pageManager.findIndexPointer(index, key, blockPointer);
            PointerPair value = new PointerPair(blockPointer, indexPointer);
            pageManager.remove(index, value, index.getColumnIndex(),updatedKeys,updatedValues,updatedOldValues);
            ((IndexInit<K>) index).remove((K) key, value);
            keys[columnIndex] = key;
            values[columnIndex] = value;
        }
        Operation updatedOperation = new Operation(OperationEnum.UPDATE, updatedKeys, updatedValues,updatedOldValues);
        Operation operation = new Operation(OperationEnum.REMOVE, keys, values,null);
        indexSnapshot.addOperation(updatedOperation);
        indexSnapshot.addOperation(operation);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void updateIndex(Entry entry, BlockPointer newBlockPointer, BlockPointer oldBlockPointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        PointerPair[] oldValues = new PointerPair[indexes.length];
        for (IndexInit<?> index : this.indexes) {
            if(index == null) continue;
            int columnIndex = index.getColumnIndex();
            K key = this.getValidatedKey(entry, index, columnIndex);
            BlockPointer indexPointer = pageManager.findIndexPointer(index, (K) key, oldBlockPointer);
            pageManager.update(indexPointer , newBlockPointer, key, columnIndex);
            PointerPair newValue = new PointerPair(newBlockPointer, indexPointer);
            PointerPair oldValue = new PointerPair(oldBlockPointer, indexPointer);
            if(index.isUnique())((IndexInit<K>) index).update(key, newValue);
            else ((IndexInit<K>) index).update(key, newValue, oldValue);
            keys[columnIndex] = key;
            values[columnIndex] = newValue;
            oldValues[columnIndex] = oldValue;
        }
        Operation operation = new Operation(OperationEnum.UPDATE, keys, values, oldValues);
        indexSnapshot.addOperation(operation);
    }

    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> K getValidatedKey(Entry entry, IndexInit<?> index, int columnIndex) {
        Object key = entry.get(columnIndex);
        DataType type = schema.getTypes()[columnIndex];
        Class<?> expectedClass = type.getJavaClass();
        if (key == null && !index.isNullable()) {
            throw new IllegalArgumentException("Null key not allowed at column " + columnIndex);
        }
        if (key != null && !expectedClass.isInstance(key)) {
            throw new IllegalArgumentException(
                "Invalid secondary key type at column " + columnIndex +
                ": expected " + expectedClass.getName() +
                ", but got " + key.getClass().getName()
            );
        }
        return (K) key;
    }

    public IndexInit<?>[] getIndexes() { return this.indexes; }
    public int getPages(int columnIndex) { 
        return this.tableSnapshots[columnIndex].getNumOfPages(); 
    }
    public void addOnePage(int columnIndex) { 
        this.tableSnapshots[columnIndex].addOnePage();
    }
    public void removeOnePage(int columnIndex) { 
        this.tableSnapshots[columnIndex].removeOnePage();
    }

    public Set<String> getDeletedPagesSet(int columnIndex) { 
        return this.tableSnapshots[columnIndex].getDeletedPageIDSet(); 
    }
    public int getDeletedPages(int columnIndex) { 
        return this.tableSnapshots[columnIndex].getDeletedPages(); 
    }
    public void addOneDeletedPage(String pageKey, int columnIndex) { 
        this.tableSnapshots[columnIndex].addDeletedPage(pageKey); 
    }
    public void removeOneDeleted(String pageKey, int columnIndex) { this.tableSnapshots[columnIndex].removeDeletedPage(pageKey); }
    public void clearDeletedPages(int columnIndex) { this.tableSnapshots[columnIndex].clearDeletedPages(); }
    public void beginTransaction(){
        for (int i = 0; i < tableSnapshots.length; i++) {
            if(tableSnapshots[i] == null) continue;
            tableSnapshots[i].beginTransaction();
        }
        indexSnapshot.beginTransaction();
    }
    public void commit() {
        for (int i = 0; i < tableSnapshots.length; i++) {
            if(tableSnapshots[i] == null) continue;
            tableSnapshots[i].commit();
        }
        indexSnapshot.commit();
    }
    public void rollback() {
        indexSnapshot.rollback(indexes);
        for (int i = 0; i < tableSnapshots.length; i++) {
            if(tableSnapshots[i] == null) continue;
            tableSnapshots[i].rollback();
        }
    }
}