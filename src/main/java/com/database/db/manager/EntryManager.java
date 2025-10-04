package com.database.db.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import com.database.db.Database;
import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Condition.WhereClause;
import com.database.db.api.DatabaseException;
import com.database.db.api.Functions.InnerFunctions;
import com.database.db.api.Functions.endConditionalUpdate;
import com.database.db.api.Functions.selectColumn;
import com.database.db.api.Query.SelectType;
import com.database.db.api.Query.SelectionType;
import com.database.db.api.Row;
import com.database.db.api.Schema;
import com.database.db.api.UpdateFields;
import com.database.db.index.BTreeInit.BlockPointer;
import com.database.db.manager.IndexManager.IndexRecord;
import com.database.db.page.Entry;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class EntryManager {
    //==SELECTING==
    /**
     * Selects entries from the table according to a {@link WhereClause}, with optional ordering by a specific column.
     * <p>
     * The process consists of two steps:
     * <ul>
     *   <li>Entries are located using the table's index and collected with an offset ({@code begin}) and row limit ({@code limit}).</li>
     *   <li>If a {@link SelectType} with {@link SelectionType#ASCENDING} or {@link SelectionType#DESCENDING} is provided,
     *       the resulting entries are sorted in-memory by the specified column.</li>
     * </ul>
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>If {@code limit} is negative, all matching entries are returned after the {@code begin} offset.</li>
     *   <li>Sorting requires that the target column's values implement {@link Comparable}.</li>
     *   <li>{@code null} values are handled safely and placed first when ordering ascending.</li>
     * </ul>
     *
     * @param table the table to select from
     * @param whereClause condition used to filter candidate entries
     * @param begin the number of matching entries to skip before collecting results
     * @param limit the maximum number of entries to return; if negative, all matching entries are returned
     * @param type ordering information: {@link SelectionType#NORMAL} returns entries in index order,
     *             {@link SelectionType#ASCENDING} or {@link SelectionType#DESCENDING} apply an explicit column ordering
     * @return a list of entries matching the criteria, optionally ordered by a given column
     */
    public static <K extends Comparable<? super K>> List<Entry> selectEntries(Table table, WhereClause whereClause, int begin, int limit, SelectType type) {
        List<IndexRecord<K>> blockPointerList = table.selectIndex(whereClause);
        List<Entry> entries = selectionProcess(table, blockPointerList, begin, limit);
        //Apply ordering if ASC or DESC is set
        if (type.type() == SelectionType.ASCENDING || type.type() == SelectionType.DESCENDING) {
            int columnIndex = table.getSchema().getColumnIndex(type.column());
            Comparator<Entry> comparator =Comparator.comparing(
                            e -> (Comparable) e.get(columnIndex),
                            Comparator.nullsFirst(Comparator.naturalOrder()));
            if (type.type() == SelectionType.DESCENDING) {
                comparator = comparator.reversed();
            }
            entries.sort(comparator);
        }
        return entries;
    }
    private static <K extends Comparable<? super K>> List<Entry> selectionProcess(Table table, List<IndexRecord<K>> indexResult, int begin, int limit){
        List<Entry> result = new ArrayList<>();
        int index = 0;
        boolean selectAll = limit < 0;
        for (IndexRecord<K> pair : indexResult) {
            BlockPointer blockPointer = pair.value().tablePointer();
            if(!selectAll && index>=limit+begin) break;
            if(index++ < begin) continue;
            TablePage page = table.getCache().getTablePage(blockPointer.BlockID());
            result.add(page.get(blockPointer.RowOffset()));
        }
        return result;
    }
    //==INSERTION==
    /**
     * Inserts a list of rows into the specified table as entries.
     * <p>
     * For each row, the method:
     * <ul>
     *   <li>Converts the row into an {@link Entry} object.</li>
     *   <li>Validates the entry against the table schema.</li>
     *   <li>Checks foreign key constraints.</li>
     *   <li>Inserts the entry into the table using {@link #insertEntry(Table, Entry)}.</li>
     * </ul>
     * The operation is executed within a transaction. If any insertion fails,
     * the transaction is rolled back and an exception is thrown.
     * 
     * @param table the table to insert rows into
     * @param rows the list of {@link Row} objects to insert
     * @return the number of successfully inserted entries
     * @throws Exception if any row fails validation or violates constraints
     */
    public static int insertEntries(Table table, List<Row> rows){
        int result = 0;
        Database database = table.getDatabase();
        Schema schema = database.getSchema(table.getName());
        database.startTransaction("Insertion Process");
        try{
            for (Row row : rows) {
                Entry entry = Entry.prepareEntry(row.getColumns(), row.getValues(), table);
                schema.isValidEntry(entry, table);
                ForeignKeyManager.foreignKeyCheck(database.getDBMS(), database, table.getName(), entry);
                insertEntry(table, entry);
                result++;
            }
        }catch(Exception e){
            database.rollBack();
            throw e;
        }
        database.commit();
        return result;
    }
    /**
     * Inserts a single entry into the specified table.
     * <p>
     * The method ensures that the entry is added to the last page of the table if there is capacity;
     * otherwise, a new page is created. It also updates the table's index and cache.
     * <p>
     * The insertion process includes:
     * <ul>
     *   <li>Ensuring there is at least one page in the table.</li>
     *   <li>Adding the entry to the last page if there is space.</li>
     *   <li>Creating a new page if the last page is full, and inserting the entry there.</li>
     *   <li>Updating the table index and cache accordingly.</li>
     * </ul>
     *
     * @param table the table where the entry should be inserted
     * @param entry the entry to insert
     * @throws IllegalArgumentException if the entry is invalid according to the table schema
     */
    public static void insertEntry(Table table, Entry entry) {
        if(table.getPages()==0) table.addOnePage();
        TablePage page = table.getCache().getLastTablePage();
        if(page.size() < page.getCapacity()){
            EntryManager.insertionProcess(table, entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().getLastTablePage();
        EntryManager.insertionProcess(table, entry, page);
    }
    /**
     * Handles the low-level insertion of an entry into a specific table page.
     * <p>
     * Adds the entry to the page, updates the table index with the entry's location,
     * and refreshes the page in the table cache.
     *
     * @param table the table containing the page
     * @param entry the entry to insert
     * @param page the page where the entry should be added
     */
    private static void insertionProcess(Table table, Entry entry, TablePage page) {
        page.add(entry);
        table.insertIndex(entry, new BlockPointer(page.getPageID(), (short)(page.size()-1)));
        table.getCache().putTablePage(page);
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
    public static <K extends Comparable<? super K>> int deleteEntry(Table table, WhereClause whereClause, int limit){
        int result = -1;
        table.getDatabase().startTransaction("Deletion Process");
        try{
            result = deletion(table, whereClause, limit);
        }catch(Exception e){
            table.getDatabase().rollBack();
            throw e;
        }
        table.getDatabase().commit();
        return result;
    }
    private static <K extends Comparable<? super K>> int deletion(Table table, WhereClause whereClause, int limit){
        List<IndexRecord<K>> indexResult = table.selectIndex(whereClause);
        boolean deleteAll = limit < 0;
        int deletedCount = 0;
        for (IndexRecord<K> value : indexResult) {
            if(!deleteAll && deletedCount>=limit)return deletedCount;
            BlockPointer pointer = table.searchIndex(value.key(), value.columnIndex()).get(0).value.tablePointer();
            Entry entryToDelete = table.getCache()
                    .getTablePage(pointer.BlockID())
                    .get(pointer.RowOffset());
            boolean allowed = ForeignKeyManager.foreignKeyDeletion(table, entryToDelete);
            if (!allowed) 
                throw new IllegalStateException("Foreign key RESTRICT violation on delete.");
            TablePage page = deletionProcess(table, pointer);
            deletedCount++;
            if (page.isLastPage() && page.size() == 0) {
                table.getCache().deleteLastTablePage(page);
                continue;
            }
            replaceWithLast(table, page, pointer);
            table.getCache().putTablePage(page);
        }
        return deletedCount;
    }
    private static TablePage deletionProcess(Table table, BlockPointer pointer){
        TablePage page = table.getCache().getTablePage(pointer.BlockID());
        Entry removed = page.get(pointer.RowOffset());
        table.removeIndex(removed, pointer);
        page.remove(pointer.RowOffset());
        if (!page.isLastPage() && pointer.RowOffset() != page.size()) {
            page.swap(pointer.RowOffset(), page.size());
        }
        return page;
    }
    private static void replaceWithLast(Table table, TablePage page, BlockPointer pointer){
    if (page.isLastPage()) {
            if (pointer.RowOffset() != page.size()) {
                Entry moved = page.get(pointer.RowOffset());
                BlockPointer oldValue = new BlockPointer(page.getPageID(), (short) page.size());
                table.updateIndex(moved, pointer, oldValue);
            }
        } else {
            TablePage lastPage = table.getCache().getLastTablePage();
            int lastOffset = lastPage.size() - 1;
            Entry lastEntry = lastPage.get(lastOffset);
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short) lastOffset);
            lastPage.removeLast();
            page.add(pointer.RowOffset(), lastEntry);
            table.updateIndex(lastEntry, pointer, oldPointer);
            if (lastPage.size() == 0) {
                table.getCache().deleteLastTablePage(lastPage);
            } else {
                table.getCache().putTablePage(lastPage);
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
    public static <K extends Comparable<? super K>> int updateEntry(Table table, WhereClause whereClause, int limit, UpdateFields updates) {
        table.getDatabase().startTransaction("Updating Process");
        int result = -1;
        try{
            result = updating(table, whereClause, limit, updates);
        }catch(Exception e){
            table.getDatabase().rollBack();
            throw e;
        }
        table.getDatabase().commit();
        return result;
    }
    private static <K extends Comparable<? super K>> int updating(Table table, WhereClause whereClause, int limit, UpdateFields updates){
        int result = 0;
        List<IndexRecord<K>> indexResult = table.selectIndex(whereClause);
        boolean updateAll = limit < 0;
        int index = -1;
        for (IndexRecord<K> pair : indexResult) {
            index++;
            if(!updateAll && index>=limit)return result;
            TablePage page = table.getCache().getTablePage(pair.value().tablePointer().BlockID());
            if (page == null) continue;
            updateProcess(table, page, pair.value().tablePointer(), updates.getFunctionsList());
            table.getCache().putTablePage(page);
            result++;
        }
        return result;
    }
    private static void updateProcess(Table table, TablePage page, BlockPointer tablePointer, List<InnerFunctions> updates) {
        Entry oldEntry = page.get(tablePointer.RowOffset());
        table.removeIndex(oldEntry, tablePointer);
        Entry newEntry;
        try{
            newEntry = newEntry(table, page, oldEntry, tablePointer, updates);
        }catch(Exception e){
            table.insertIndex(oldEntry, tablePointer);
            throw e;
        }
        page.set(tablePointer.RowOffset(),newEntry);
        table.insertIndex(newEntry, tablePointer);
    }
    private static Entry newEntry(Table table, TablePage page, Entry oldEntry, BlockPointer pointer, List<InnerFunctions> updates){
        Database database = table.getDatabase();
        Object[] newValues = Arrays.copyOf(oldEntry.getEntry(), oldEntry.getEntry().length);
        applyUpdates(table, newValues, updates);
        Entry newEntry = new Entry(newValues,table.getSchema().getNumOfColumns())
            .setBitMap(table.getSchema().getNotNull());
        database.getSchema(table.getName()).isValidEntry(newEntry, table);
        ForeignKeyManager.foreignKeyCheck(database.getDBMS(), database, table.getName(), newEntry);
        ForeignKeyManager.foreignKeyUpdate(table, oldEntry, newValues);
        return newEntry;
    }
    private static void applyUpdates(Table table, Object[] values, List<InnerFunctions> updates){
        int columnsIndex = -1;
        boolean isInCondition = false;
        boolean conditionResult = false;
        for (InnerFunctions update : updates) {
            if(update instanceof selectColumn) {
                columnsIndex = table.getSchema().getColumnIndex(((selectColumn)update).column());
                continue;
            }
            if(columnsIndex<0)
                throw new DatabaseException("Invalid column to update");
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