package com.database.db.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.database.db.Database;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.Functions.InnerFunctions;
import com.database.db.api.UpdateFields;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class EntryManager {
    private Database database;
    private Table table;
    private Schema schema;
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
     * @param start the lower bound of the range filter for the target column
     * @param end the upper bound of the range filter
     * @param columnIndex the index of the column to filter and sort entries on
     * @param begin the number of matching entries to skip before starting to collect results
     * @param limit the maximum number of entries to return; if negative, all matching entries are returned
     * @return a list of entries matching the criteria, ordered in ascending fashion
     */
    public List<Entry> selectEntriesAscending(WhereClause whereClause, int begin, int limit) {
        List<BlockPointer> blockPointerList = table.findRangeIndex(whereClause);
        return this.selectionProcess(blockPointerList, begin, limit);
    }
    /**
     * Selects entries from the table in descending order based on the given column and range.
     * The entries are fetched using a range index, reversed, and returned starting from a specific offset.
     *
     * @param start the lower bound of the range filter for the target column
     * @param end the upper bound of the range filter
     * @param columnIndex the index of the column to filter and sort entries on
     * @param begin the number of matching entries to skip before starting to collect results
     * @param limit the maximum number of entries to return; if negative, all matching entries are returned
     * @return a list of entries matching the criteria, ordered in descending fashion
     */
    public List<Entry> selectEntriesDescending(WhereClause whereClause, int begin, int limit) {
        List<BlockPointer> reversed = table.findRangeIndex(whereClause);
        Collections.reverse(reversed);
        return this.selectionProcess(reversed, begin, limit);
    }
    private List<Entry> selectionProcess(List<BlockPointer> blockPointerList, int begin, int limit){
        List<Entry> result = new ArrayList<>();
        int index = 0;
        boolean selectAll = limit < 0;
        for (BlockPointer blockPointer : blockPointerList) {
            if(index++ < begin) continue;
            if(!selectAll && index>=limit+begin) break;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            result.add(page.get(blockPointer.RowOffset()));
        }
        return result;
    }
    //==INSERTION==
    /**
     * Inserts a new entry into the table. If the last page has available space, the entry
     * is added there. Otherwise, a new page is created to accommodate the entry.
     *
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
        if(!Entry.isValidEntry(entry.getEntry(),this.table.getSchema())) throw new IllegalArgumentException("Invalid Entry");
        if(table.getPages()==0) table.addOnePage();
        TablePage page = table.getCache().getLast();
        if(page.size() < page.getCapacity()){
            this.insertionProcess(entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().getLast();
        this.insertionProcess(entry, page);
    }
    private void insertionProcess(Entry entry, TablePage page) {
        page.add(entry);
        table.insertIndex(entry, new BlockPointer(page.getPageID(), (short)(page.size()-1)));
        table.getCache().put(page);
    }
    //==DELETION==
    /**
    * Deletes entries from the table that fall within a specified range on a given column.
    * The deletion uses a range index to identify target entries and removes up to the given limit.
    *
    * The method ensures page compaction by replacing deleted entries with the last entry
    * from the table when applicable. If the last page becomes empty as a result, it is removed.
    *
    * @param start the lower bound of the range filter (inclusive or exclusive depending on implementation)
    * @param end the upper bound of the range filter
    * @param columnIndex the index of the column to apply the range filter on
    * @param limit the maximum number of entries to delete; if negative, all matching entries are deleted
    * @return the number of entries successfully deleted
    */
    public int deleteEntry(WhereClause whereClause, int limit){
        List<BlockPointer> blockPointerList = table.findRangeIndex(whereClause);
        boolean deleteAll = limit < 0;
        int deletedCount = 0;
        for (BlockPointer blockPointer : blockPointerList) {
            if(!deleteAll && deletedCount>=limit)return deletedCount;
            TablePage page = this.deletionProcess(blockPointer);
            if(page == null) continue;
            deletedCount++;
            if (page.isLastPage()) {
                if (page.size() == 0) {
                    table.getCache().deleteLastPage(page);
                }
                table.getCache().put(page);
                continue;
            }
            this.replaceLastEntry(page, blockPointer);
        }
        return deletedCount;
    }
    private TablePage deletionProcess(BlockPointer blockPointer) {
        TablePage page = table.getCache().get(blockPointer.BlockID());
        if(page == null) return null;
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        if(blockPointer.RowOffset() != page.size()){
            Entry swapped = page.get(blockPointer.RowOffset());//On removal last entry gets the place of the removed one
            BlockPointer oldValue = new BlockPointer(page.getPageID(), page.size());
            table.updateIndex(swapped, blockPointer, oldValue);
        }
        return page;
    }
    private void replaceLastEntry(TablePage page,  BlockPointer blockPointer){
        TablePage lastPage = table.getCache().getLast();
        Entry lastEntry = lastPage.removeLast();
        page.add(lastEntry);
        BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short)(lastPage.size()));
        BlockPointer newPointer = new BlockPointer(page.getPageID(),(short)(page.size()-1));
        table.updateIndex(lastEntry, newPointer, oldPointer);
        table.getCache().put(page);
        if (lastPage.size() == 0) {
            table.getCache().deleteLastPage(lastPage);
        }else{
            table.getCache().put(lastPage);
        }
    }

    //==UPDATING==
    /**
     * Updates entries in the table that fall within a specified range on a given column.
     * For each matching entry, a set of update functions is applied to specified columns.
     *
     * The method:
     * <ul>
     *   <li>Uses a range index to locate target entries based on the given column and range.</li>
     *   <li>Removes each matching entry from the page and index.</li>
     *   <li>Applies provided transformation functions to specified columns.</li>
     *   <li>Validates and reinserts the updated entry into the page and index.</li>
     *   <li>Updates the page in the cache.</li>
     * </ul>
     *
     * @param start the lower bound of the range filter for the target column
     * @param end the upper bound of the range filter
     * @param columnIndex the index of the column to filter entries on
     * @param limit the maximum number of entries to update; if negative, all matching entries are updated
     * @param updates a list of update operations, each mapping a column name to a function that computes the new value
     * @return the number of entries successfully updated
     * @throws IllegalArgumentException if any updated entry is invalid or if a column name in updates is not found
     */
    public int updateEntry(WhereClause whereClause, int limit, UpdateFields updates) {
        int result = 0;
        List<BlockPointer> blockPointerList = table.findRangeIndex(whereClause);
        boolean updateAll = limit < 0;
        int index = -1;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!updateAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            if (page == null) continue;
            this.updateProcess(page, blockPointer, updates.getFunctionsList());
            table.getCache().put(page);
            result++;
        }
        return result;
    }
    private void updateProcess(TablePage page, BlockPointer blockPointer,  List<Map.Entry<String,InnerFunctions>> updates) {
        BlockPointer oldBlockPointer = new BlockPointer(page.getPageID(), (short)(page.size()-1));
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        if(blockPointer.RowOffset() != page.size())
            table.updateIndex(page.get(blockPointer.RowOffset()), blockPointer, oldBlockPointer);
        Object[] values = removed.getEntry();
        values = this.applyUpdates(values, updates);
        if(!Entry.isValidEntry(values, this.table.getSchema()))
            throw new IllegalArgumentException("Unable to update, final entry is invalid.");
        Entry updatedEntry = new Entry(values,this.schema.getNumOfColumns())
            .setBitMap(this.table.getSchema().getNotNull());
        page.add(updatedEntry);
        BlockPointer newPointer = new BlockPointer(page.getPageID(),(short)(page.size()-1));
        table.insertIndex(updatedEntry, newPointer);
    }
    private Object[] applyUpdates(Object[] values, List<Map.Entry<String,InnerFunctions>> updates){
        ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(this.table.getSchema().getNames()));
        for (Map.Entry<String, InnerFunctions> update : updates) {
            int index = columnNames.indexOf(update.getKey());
            if(index<0)
                throw new IllegalArgumentException("Invalid column to update: "+update.getKey());
            values[index] = update.getValue().apply(this.table.getSchema(), values, index);
        }
        return values;
    }
}
