package com.database.tttdb.core.manager;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.database.tttdb.api.Condition;
import com.database.tttdb.api.Condition.Clause;
import com.database.tttdb.api.Condition.WhereClause;
import com.database.tttdb.core.FileIO;
import com.database.tttdb.core.cache.IndexSnapshot;
import com.database.tttdb.core.cache.TableSnapshot;
import com.database.tttdb.core.cache.IndexSnapshot.Operation;
import com.database.tttdb.core.cache.IndexSnapshot.OperationEnum;
import com.database.tttdb.core.cache.PageKey;
import com.database.tttdb.core.index.IndexFactory;
import com.database.tttdb.core.index.IndexInit;
import com.database.tttdb.core.index.Pair;
import com.database.tttdb.core.index.IndexFactory.IndexKind;
import com.database.tttdb.core.index.IndexInit.BlockPointer;
import com.database.tttdb.core.index.IndexInit.PointerPair;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.page.IndexPage;
import com.database.tttdb.core.table.DataType;
import com.database.tttdb.core.table.Table;
import com.database.tttdb.core.table.TableSchema;

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
    /**
     * Evaluates a WHERE clause using available indexes.
     * Combines per-condition index hits with AND/OR semantics.
     *
     * @param whereClause parsed clause tree. If null, returns full-scan fallback via preferred index.
     * @param <K> key type
     * @return ordered list of index records matching the clause
     */
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
    /**
     * Locates index pointer pairs for an exact key on a column.
     * Falls back to sequential scan if the column is not indexed.
     *
     * @param key lookup key
     * @param columnIndex column id
     * @param <K> key type
     * @return list of (key, PointerPair) results, possibly empty
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<Pair<K, PointerPair>> findBlock(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex);
        return ((IndexInit<K>)this.indexes[columnIndex]).search(key);
    }
    /**
     * Tests existence of a key in a column.
     * Uses the column index if present, else a sequential probe.
     *
     * @param key key to test
     * @param columnIndex column id
     * @param <K> key type
     * @return true if at least one row with key exists
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        if(this.indexes[columnIndex] == null) return !SequentialOperations.sequentialRangeSearch(table, key, key, columnIndex).isEmpty();
        return ((IndexInit<K>)this.indexes[columnIndex]).isKey(key);
    }

    /**
     * Inserts index entries for a newly inserted table row.
     * Appends an INSERT operation to the index snapshot for rollback.
     *
     * Invariants:
     * - Arrays are written by index position i, matching {@code indexes[i]}.
     *
     * @param entry row data
     * @param tablePointer location of the row in table storage
     * @param <K> key type
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void insertIndex(Entry entry, BlockPointer tablePointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            IndexInit<?> index = indexes[i];
            if(index == null) continue;
            assert index.getColumnIndex() == i : "Index position and columnIndex diverged";
            int columnIndex = index.getColumnIndex();
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
            BlockPointer indexPointer = this.pageManager.insert(tablePointer, key, index.getColumnIndex());
            PointerPair value = new PointerPair(tablePointer,indexPointer);
            ((IndexInit<K>) index).insert(key, value);
            keys[i] = key;
            values[i] = value;
        }
        Operation operation = new Operation(OperationEnum.INSERT, keys, values,null);
        indexSnapshot.addOperation(operation);
    }

    /**
     * Removes index entries for a deleted table row.
     * If a swap occurs during compaction, records an UPDATE op before the REMOVE
     * so rollback can undo the swap then reinsert.
     *
     * @param entry row being deleted
     * @param blockPointer table pointer of the row
     * @param <K> key type
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void removeIndex(Entry entry, BlockPointer blockPointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        Object[] updatedKeys = new Object[indexes.length];
        PointerPair[] updatedValues = new PointerPair[indexes.length];
        PointerPair[] updatedOldValues = new PointerPair[indexes.length];
        boolean indexChanged = false;
        for (int i = 0; i < indexes.length; i++) {
            IndexInit<?> index = indexes[i];
            if(index == null) continue;
            assert index.getColumnIndex() == i : "Index position and columnIndex diverged";
            int columnIndex = index.getColumnIndex();
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
            BlockPointer indexPointer = pageManager.findIndexPointer(index, key, blockPointer);
            PointerPair value = new PointerPair(blockPointer, indexPointer);
            indexChanged |= pageManager.remove(index, value, index.getColumnIndex(),updatedKeys,updatedValues,updatedOldValues);
            ((IndexInit<K>) index).remove((K) key, value);
            keys[i] = key;
            values[i] = value;
        }
        Operation updatedOperation = new Operation(OperationEnum.UPDATE, updatedKeys, updatedValues,updatedOldValues);
        Operation operation = new Operation(OperationEnum.REMOVE, keys, values,null);
        if(indexChanged) indexSnapshot.addOperation(updatedOperation);
        indexSnapshot.addOperation(operation);
    }

    /**
     * Updates index entries for a moved or modified row.
     * Records an UPDATE operation for rollback.
     *
     * @param entry new entry state
     * @param newBlockPointer new table pointer
     * @param oldBlockPointer old table pointer
     * @param <K> key type
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void updateIndex(Entry entry, BlockPointer newBlockPointer, BlockPointer oldBlockPointer){
        Object[] keys = new Object[indexes.length];
        PointerPair[] values = new PointerPair[indexes.length];
        PointerPair[] oldValues = new PointerPair[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            IndexInit<?> index = indexes[i];
            if(index == null) continue;
            assert index.getColumnIndex() == i : "Index position and columnIndex diverged";
            int columnIndex = index.getColumnIndex();
            K key = IndexUtils.getValidatedKey(entry, index, columnIndex,columnTypes[columnIndex]);
            BlockPointer indexPointer = pageManager.findIndexPointer(index, (K) key, oldBlockPointer);
            pageManager.update(indexPointer , newBlockPointer, key, columnIndex);
            PointerPair newValue = new PointerPair(newBlockPointer, indexPointer);
            PointerPair oldValue = new PointerPair(oldBlockPointer, indexPointer);
            if(index.isUnique())((IndexInit<K>) index).update(key, newValue);
            else ((IndexInit<K>) index).update(key, newValue, oldValue);
            keys[i] = key;
            values[i] = newValue;
            oldValues[i] = oldValue;
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

    public Set<PageKey> getDeletedPagesSet(int columnIndex) { 
        return this.tableSnapshots[columnIndex].getDeletedPageIDSet(); 
    }
    public int getDeletedPages(int columnIndex) { 
        return this.tableSnapshots[columnIndex].getDeletedPages(); 
    }
    public void addOneDeletedPage(PageKey pageKey, int columnIndex) { 
        this.tableSnapshots[columnIndex].addDeletedPage(pageKey); 
    }
    public void removeOneDeleted(PageKey pageKey, int columnIndex) { this.tableSnapshots[columnIndex].removeDeletedPage(pageKey); }
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