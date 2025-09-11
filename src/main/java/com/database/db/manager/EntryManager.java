package com.database.db.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.database.db.Database;
import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.Functions.InnerFunctions;
import com.database.db.api.Functions.endConditionalUpdate;
import com.database.db.api.Functions.selectColumn;
import com.database.db.api.UpdateFields;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.page.Entry;
import com.database.db.page.TablePage;
import com.database.db.table.SchemaInner;
import com.database.db.table.Table;
import com.database.db.table.Table.IndexRecord;

public class EntryManager {
    private Database database;
    private Table table;
    private SchemaInner schema;
    public EntryManager(){}
    public void selectDatabase(Database database){
        this.database = database;
    }
    public void selectTable(String tableName){
        this.table = this.database.getTable(tableName);
        this.schema = this.table.getSchema();
    }
    public Table getTable(){
        return this.table;
    }
    //==SELECTING==
    /**
     * Selects entries from the table in ascending order based on the given column and range.
     * The entries are fetched using a range index and returned starting from a specific offset.
     *
     * @param whereClause contains a condition, it is used to search for entries
     * @param begin the number of matching entries to skip before starting to collect results
     * @param limit the maximum number of entries to return; if negative, all matching entries are returned
     * @return a list of entries matching the criteria, ordered in ascending fashion
     */
    public <K extends Comparable<? super K>> List<Entry> selectEntriesAscending(WhereClause whereClause, int begin, int limit) {
        List<IndexRecord<K>> blockPointerList = table.findRangeIndex(whereClause);
        return this.selectionProcess(blockPointerList, begin, limit);
    }
    /**
     * Selects entries from the table in descending order based on the given column and range.
     * The entries are fetched using a range index, reversed, and returned starting from a specific offset.
     *
     * @param whereClause contains a condition, it is used to search for entries
     * @param begin the number of matching entries to skip before starting to collect results
     * @param limit the maximum number of entries to return; if negative, all matching entries are returned
     * @return a list of entries matching the criteria, ordered in descending fashion
     */
    public <K extends Comparable<? super K>> List<Entry> selectEntriesDescending(WhereClause whereClause, int begin, int limit) {
        List<IndexRecord<K>> reversed = table.findRangeIndex(whereClause);
        Collections.reverse(reversed);
        return this.selectionProcess(reversed, begin, limit);
    }
    private <K extends Comparable<? super K>> List<Entry> selectionProcess(List<IndexRecord<K>> indexResult, int begin, int limit){
        List<Entry> result = new ArrayList<>();
        int index = 0;
        boolean selectAll = limit < 0;
        for (IndexRecord<K> pair : indexResult) {
            BlockPointer blockPointer = pair.value().tablePointer();
            if(index++ < begin) continue;
            if(!selectAll && index>=limit+begin) break;
            TablePage page = table.getCache().tableCache.get(blockPointer.BlockID());
            result.add(page.get(blockPointer.RowOffset()));
        }
        return result;
    }
    //==INSERTION==
    /**
     * Inserts a new entry into the table. If the last page has available space, the entry
     * is added there. Otherwise, a new page is created to accommodate the entry.
     * The method performs the following:
     * <ul>
     *   <li>Validates the entry against the table schema.</li>
     *   <li>Ensures there is at least one page in the table.</li>
     *   <li>Inserts the entry into the last page if it has capacity.</li>
     *   <li>If the last page is full, adds a new page and inserts the entry there.</li>
     *   <li>Updates the index and cache as part of the insertion process.</li>
     * </ul>
     *
     * @param entry the entry to insert into the table
     * @throws IllegalArgumentException if the entry is invalid according to the table schema
     */
    public void insertEntry(Entry entry) {
        if(table.getPages()==0) table.addOnePage();
        TablePage page = table.getCache().tableCache.getLast();
        if(page.size() < page.getCapacity()){
            this.insertionProcess(entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().tableCache.getLast();
        this.insertionProcess(entry, page);
    }
    private void insertionProcess(Entry entry, TablePage page) {
        page.add(entry);
        table.insertIndex(entry, new BlockPointer(page.getPageID(), (short)(page.size()-1)));
        table.getCache().tableCache.put(page);
    }
    //==DELETION==
    /**
    * Deletes entries from the table that fall within a specified range on a given column.
    * The deletion uses a range index to identify target entries and removes up to the given limit.
    * The method ensures page compaction by replacing deleted entries with the last entry
    * from the table when applicable. If the last page becomes empty as a result, it is removed.
    * @param whereClause contains a condition, it is used to search for entries
    * @param limit the maximum number of entries to delete; if negative, all matching entries are deleted
    * @return the number of entries successfully deleted
    */
    public <K extends Comparable<? super K>> int deleteEntry(WhereClause whereClause, int limit){
        List<IndexRecord<K>> indexResult = table.findRangeIndex(whereClause);
        boolean deleteAll = limit < 0;
        int deletedCount = 0;
        for (IndexRecord<K> value : indexResult) {
            if(!deleteAll && deletedCount>=limit)return deletedCount;
            BlockPointer pointer = table.searchIndex(value.key(), value.columnIndex()).get(0).value.tablePointer();
            Entry entryToDelete = table.getCache().tableCache
                    .get(pointer.BlockID())
                    .get(pointer.RowOffset());
            boolean allowed = ForeignKeyManager.foreignKeyDeletion(this, table, entryToDelete);
            if (!allowed) 
                throw new IllegalStateException("Foreign key RESTRICT violation on delete.");
            TablePage page = deletionProcess(pointer);
            deletedCount++;
            if (page.isLastPage() && page.size() == 0) {
                table.getCache().tableCache.deleteLastPage(page);
                continue;
            }
            // --- Handle replacements ---
            this.replaceWithLast(page, pointer);
            table.getCache().tableCache.put(page);
        }
        return deletedCount;
    }
    private TablePage deletionProcess(BlockPointer pointer){
        TablePage page = table.getCache().tableCache.get(pointer.BlockID());
        // --- Remove entry + indexes ---
        Entry removed = page.get(pointer.RowOffset());
        table.removeIndex(removed, pointer);
        page.remove(pointer.RowOffset());
        // Reverse internal swap if not last page
        if (!page.isLastPage() && pointer.RowOffset() != page.size()) {
            page.swap(pointer.RowOffset(), page.size());
        }
        return page;
    }
    private void replaceWithLast(TablePage page, BlockPointer pointer){
    if (page.isLastPage()) {
            // Case A: deleted from middle of last page -> update swapped entry
            if (pointer.RowOffset() != page.size()) {
                Entry moved = page.get(pointer.RowOffset());

                // old location was last slot BEFORE removal
                BlockPointer oldValue = new BlockPointer(page.getPageID(), (short) page.size());

                table.updateIndex(moved, pointer, oldValue);
            }
        } else {
            // Case B: deleted from non-last page -> move last entry of last page here
            TablePage lastPage = table.getCache().tableCache.getLast();
            int lastOffset = lastPage.size() - 1;
            Entry lastEntry = lastPage.get(lastOffset);
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short) lastOffset);

            lastPage.removeLast();

            // ✅ overwrite freed slot directly (your add does NOT shift)
            page.add(pointer.RowOffset(), lastEntry);

            // update index mapping for lastEntry
            table.updateIndex(lastEntry, pointer, oldPointer);

            // save changes
            if (lastPage.size() == 0) {
                table.getCache().tableCache.deleteLastPage(lastPage);
            } else {
                table.getCache().tableCache.put(lastPage);
            }
        }
    }
    //==UPDATING==
    /**
     * Updates entries in the table that fall within a specified range on a given column.
     * For each matching entry, a set of update functions is applied to specified columns.
     * The method:
     * <ul>
     *   <li>Uses a range index to locate target entries based on the given column and range.</li>
     *   <li>Removes each matching entry from the page and index.</li>
     *   <li>Applies provided transformation functions to specified columns.</li>
     *   <li>Validates and reinserts the updated entry into the page and index.</li>
     *   <li>Updates the page in the cache.</li>
     * </ul>
     *
     * @param whereClause contains a condition, it is used to search for entries
     * @param limit the maximum number of entries to update; if negative, all matching entries are updated
     * @param updates a list of update operations, each mapping a column name to a function that computes the new value
     * @return the number of entries successfully updated
     * @throws IllegalArgumentException if any updated entry is invalid or if a column name in updates is not found
     */
    public <K extends Comparable<? super K>> int updateEntry(WhereClause whereClause, int limit, UpdateFields updates) {
        int result = 0;
        List<IndexRecord<K>> indexResult = table.findRangeIndex(whereClause);
        boolean updateAll = limit < 0;
        int index = -1;
        for (IndexRecord<K> pair : indexResult) {
            index++;
            if(!updateAll && index>=limit)return result;
            TablePage page = table.getCache().tableCache.get(pair.value().tablePointer().BlockID());
            if (page == null) continue;
            this.updateProcess(page, pair.value().tablePointer(), updates.getFunctionsList());
            table.getCache().tableCache.put(page);
            result++;
        }
        return result;
    }
    private void updateProcess(TablePage page, BlockPointer tablePointer, List<InnerFunctions> updates) {
        Entry oldEntry = page.get(tablePointer.RowOffset());
        table.removeIndex(oldEntry, tablePointer);
        Entry newEntry;
        try{
            newEntry = this.newEntry(page, oldEntry, tablePointer, updates);
        }catch(Exception e){
            table.insertIndex(oldEntry, tablePointer);
            throw e;
        }
        page.set(tablePointer.RowOffset(),newEntry);
        table.insertIndex(newEntry, tablePointer);
    }
    private Entry newEntry(TablePage page, Entry oldEntry, BlockPointer pointer, List<InnerFunctions> updates){
        Object[] newValues = Arrays.copyOf(oldEntry.getEntry(), oldEntry.getEntry().length);
        this.applyUpdates(newValues, updates);
        Entry newEntry = new Entry(newValues,this.schema.getNumOfColumns())
            .setBitMap(this.table.getSchema().getNotNull());
        database.getSchema(table.getName()).isValidEntry(newEntry, table);
        ForeignKeyManager.foreignKeyCheck(database.getDBMS(), database, table.getName(), newEntry);
        ForeignKeyManager.foreignKeyUpdate(this, table, oldEntry, newValues);
        return newEntry;
    }
    private void applyUpdates(Object[] values, List<InnerFunctions> updates){
        int columnsIndex = -1;
        boolean isInCondition = false;
        boolean conditionResult = false;
        for (InnerFunctions update : updates) {
            if(update instanceof selectColumn) {
                columnsIndex = table.getSchema().getColumnIndex(((selectColumn)update).column());
                continue;
            }
            if(columnsIndex<0)
                throw new IllegalArgumentException("Invalid column to update");
            if(update instanceof UpdateCondition){
                conditionResult = ((UpdateCondition)update).isTrue(table.getSchema(), values);
                if(conditionResult) isInCondition = true;
            }
            if(update instanceof endConditionalUpdate) isInCondition = false;
            if(isInCondition){
                if(conditionResult){
                    values[columnsIndex] = update.apply(table.getSchema(), values, columnsIndex);
                }
            }else{
                values[columnsIndex] = update.apply(table.getSchema(), values, columnsIndex);
            }
        }
    }
}