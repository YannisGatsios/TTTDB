package com.database.db.core.manager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.database.db.api.Condition;
import com.database.db.api.Condition.Clause;
import com.database.db.api.Condition.WhereClause;
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
    private final TableSchema schema;
    private final IndexInit<?>[] indexes;
    private final DataType[] columnTypes;
    private final IndexPageManager pageManager;
    private final TableSnapshot[] tableSnapshots;
    private final IndexSnapshot indexSnapshot;

    public IndexManager(Table table) {
        this.table = table;
        this.schema = table.getSchema();
        this.indexes = new IndexInit<?>[schema.getNumOfColumns()];
        this.columnTypes = schema.getTypes();

        this.pageManager = new IndexPageManager(table);
        int primaryIndex = schema.getPrimaryIndex();
        boolean[] uniqueIndexes = schema.getUniqueIndex();
        boolean[] secondaryIndexes = schema.getSecondaryIndex();
        this.tableSnapshots = new TableSnapshot[indexes.length];
        for (int i = 0;i<this.indexes.length;i++) {
            if(primaryIndex == i) this.indexes[i] = IndexFactory.createIndex(IndexKind.PRIMARY, table, i, columnTypes[i]);
            else if (uniqueIndexes[i]) this.indexes[i] = IndexFactory.createIndex(IndexKind.UNIQUE, table, i, columnTypes[i]);
            else if (secondaryIndexes[i]) this.indexes[i] = IndexFactory.createIndex(IndexKind.SECONDARY, table, i, columnTypes[i]);
            if(indexes[i] != null) this.tableSnapshots[i] = new TableSnapshot();
        }
        this.indexSnapshot = new IndexSnapshot();
    }
    public void initialize(){
        for (int i = 0; i < indexes.length; i++) {
            IndexInit<?> idx = indexes[i];
            if (idx == null) continue;
            idx.initialize(table);
            int numPages = FileIO.getNumOfPages(table.getIndexPath(i), IndexPage.sizeOfEntry(table, i));
            tableSnapshots[i].setNumOfPages(numPages);
        }
    }

    public boolean isIndexed(int columnIndex){
        return this.indexes[columnIndex] != null;
    }
    public Object getMax(int columnIndex){
        if(this.indexes[columnIndex] == null) return SequentialOperations.getMaxSequential(table,columnIndex);
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
    public <K extends Comparable<? super K>> List<IndexRecord<K>> findRangeIndex(WhereClause whereClause) {
        if (whereClause == null) return IndexUtils.noCondition(table, indexes, schema.getPreferredIndexColumn());

        List<Map.Entry<Clause, Condition<WhereClause>>> clauses = whereClause.getConditions();
        List<IndexRecord<K>> previous = Collections.emptyList();

        for (var entry : clauses) {
            Clause clause = entry.getKey();
            List<IndexRecord<K>> results = IndexUtils.evaluateCondition(table, indexes, entry.getValue());
            if (clause == Clause.FIRST) previous = results;
            else if (clause == Clause.OR) previous = IndexUtils.mergeOr(previous, results);
            else if (clause == Clause.AND) previous = IndexUtils.mergeAnd(previous, results);
        }
        return previous;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<Pair<K, PointerPair>> findBlock(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex);
        return ((IndexInit<K>)this.indexes[columnIndex]).search(key);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return !SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex).isEmpty();
        return ((IndexInit<K>)this.indexes[columnIndex]).isKey(key);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void insertIndex(Entry entry, BlockPointer tablePointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        for (IndexInit<?> index : this.indexes) {
            if(index == null) continue;
            int columnIndex = index.getColumnIndex();
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
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
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
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
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
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

    public IndexInit<?>[] getIndexes() { return this.indexes; }
    public int getPages(int columnIndex) {
        TableSnapshot ts = tableSnapshots[columnIndex];
        return ts != null ? ts.getNumOfPages() : 0;
    }
    public void addOnePage(int columnIndex) {
        TableSnapshot ts = tableSnapshots[columnIndex];
        if (ts != null) ts.addOnePage();
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
        forEachSnapshot(TableSnapshot::beginTransaction);
        indexSnapshot.beginTransaction();
    }
    public void commit() {
        forEachSnapshot(TableSnapshot::commit);
        indexSnapshot.commit();
    }
    public void rollback() {
        indexSnapshot.rollback(indexes);
        forEachSnapshot(TableSnapshot::rollback);
    }
    private void forEachSnapshot(Consumer<TableSnapshot> action) {
        for (TableSnapshot ts : tableSnapshots)
            if (ts != null) action.accept(ts);
    }
}