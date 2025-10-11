package com.database.db.core.manager;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.database.db.api.Condition;
import com.database.db.api.Condition.Clause;
import com.database.db.api.Condition.Conditions;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.ConditionUtils;
import com.database.db.core.FileIO;
import com.database.db.core.cache.IndexSnapshot;
import com.database.db.core.cache.TableSnapshot;
import com.database.db.core.cache.IndexSnapshot.Operation;
import com.database.db.core.cache.IndexSnapshot.OperationEnum;
import com.database.db.core.index.IndexFactory;
import com.database.db.core.index.IndexInit;
import com.database.db.core.index.Pair;
import com.database.db.core.index.IndexFactory.IndexKind;
import com.database.db.core.index.IndexInit.BlockPointer;
import com.database.db.core.index.IndexInit.PointerPair;
import com.database.db.core.page.Entry;
import com.database.db.core.page.IndexPage;
import com.database.db.core.table.DataType;
import com.database.db.core.table.Table;
import com.database.db.core.table.TableSchema;

public class IndexManager {

    private final Table table;
    TableSchema schema;
    private final IndexInit<?>[] indexes;
    public final IndexPageManager pageManager;
    private final TableSnapshot[] tableSnapshots;
    private final IndexSnapshot indexSnapshot;

    public IndexManager(Table table) {
        this.table = table;
        this.schema = table.getSchema();
        this.indexes = new IndexInit<?>[schema.getNumOfColumns()];

        this.pageManager = new IndexPageManager(table);
        int PrimaryIndex = schema.getPrimaryIndex();
        boolean[] UniqueIndexes = schema.getUniqueIndex();
        boolean[] SecondaryIndexes = schema.getSecondaryIndex();
        DataType[] types = schema.getTypes();
        for (int i = 0;i<this.indexes.length;i++) {
            if(PrimaryIndex == i) this.indexes[i] = IndexFactory.createIndex(IndexKind.PRIMARY, table, i, types[i]);
            else if (UniqueIndexes[i]) this.indexes[i] = IndexFactory.createIndex(IndexKind.UNIQUE, table, i, types[i]);
            else if (SecondaryIndexes[i]) this.indexes[i] = IndexFactory.createIndex(IndexKind.SECONDARY, table, i, types[i]);
        }

        int cols = schema.getNumOfColumns();
        this.tableSnapshots = new TableSnapshot[cols];
        this.indexSnapshot = new IndexSnapshot();
        for (int i = 0; i < cols; i++) {
            if(indexes[i] != null){
                this.tableSnapshots[i] = new TableSnapshot();
            }
        }
    }
    public void initialize(){
        for (IndexInit<?> index : indexes) {
            if(index != null) index.initialize(table);
        }
        for (int i = 0;i<this.indexes.length;i++) {
            if(indexes[i] != null){
                int numOfPages =  FileIO.getNumOfPages(table.getIndexPath(i), IndexPage.sizeOfEntry(table, i));
                tableSnapshots[i].setNumOfPages(numOfPages);
            }
        }
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
        List<Map.Entry<Clause, Condition<WhereClause>>> clauses = whereClause.getConditions();
        List<IndexRecord<K>> previousPairList = new ArrayList<>();
        for (Map.Entry<Clause, Condition<WhereClause>> conditionEntry : clauses) {
            Condition<WhereClause> condition = conditionEntry.getValue();
            EnumMap<Conditions, Object> conditionList = condition.getConditions();

            ConditionUtils.RangeBounds bounds = ConditionUtils.extractRange(conditionList);
            Object start = bounds.start();
            Object end = bounds.end();

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
        int columnIndex = schema.getPreferredIndexColumn();

        IndexInit<K> index = (IndexInit<K>) indexes[columnIndex];
        List<Pair<K, PointerPair>> indexResult =
            index == null
                ? SequentialOperations.sequentialRangeSearch(table, null, null, columnIndex)
                : index.rangeSearch(null, null);

        List<IndexRecord<K>> result = new ArrayList<>(indexResult.size());
        for (Pair<K, PointerPair> pair : indexResult) {
            result.add(new IndexRecord<>(pair.key, pair.value, columnIndex));
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