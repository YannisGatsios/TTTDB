package com.database.db.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.Database;
import com.database.db.FileIO;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Table;
import com.database.db.CRUD.Functions.InnerFunctions;

public class EntryManager {
    private static final Logger logger = Logger.getLogger(EntryManager.class.getName());
    private Database database;
    private Table table;
    public EntryManager(){}
    public void selectDatabase(Database database){
        this.database = database;
    }
    public void selectTable(String tableName){
        this.table = this.database.getTable(tableName);
    }
    //==SELECTING==
    public List<Entry> selectEntriesAscending(Object start,Object end, int columnIndex,int begin, int limit) {
        List<BlockPointer> blockPointerList = table.findRangeIndex(start, end, columnIndex);
        return this.selectionProcess(blockPointerList, begin, limit);
    }
    public List<Entry> selectEntriesDescending(Object start,Object end, int columnIndex,int begin, int limit) {
        List<BlockPointer> reversed = table.findRangeIndex(start, end, columnIndex);
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
    public void insertEntry(Entry entry) {
        if(!Entry.isValidEntry(entry.getEntry(),table.getSchema())) throw new IllegalArgumentException("Invalid Entry");
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
    }
    //==DELETION==
    public List<Entry> deleteEntry(Object start, Object end, int columnIndex, int limit){
        ArrayList<Entry> result = new ArrayList<>();
        List<BlockPointer> blockPointerList = table.findRangeIndex(start,end,columnIndex);
        boolean deleteAll = limit < 0;
        int index = 0;
        for (BlockPointer blockPointer : blockPointerList) {
            if(!deleteAll && index++>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            if (page == null) continue;
            result.add(this.deletionProcess(page, blockPointer));
            if (page.isLastPage() && page.size() == 0) {
                this.removeLastPage(page);
                continue;
            }
            if(page.isLastPage()) continue;
            TablePage lastPage = table.getCache().getLast();
            Entry lastEntry = lastPage.removeLast();
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short)(lastPage.size()));
            page.add(lastEntry);
            table.updateIndex(lastEntry, blockPointer, oldPointer);
            if (lastPage.size() == 0) {
                this.removeLastPage(lastPage);
            }
        }
        return result;
    }
    private Entry deletionProcess(TablePage page, BlockPointer blockPointer) {
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        if(page.size() > 0 && blockPointer.RowOffset() != page.size()){
            Entry swapped = page.get(blockPointer.RowOffset());//On removal last entry gets the place of the removed one
            BlockPointer oldValue = new BlockPointer(page.getPageID(), page.size());
            table.updateIndex(swapped, blockPointer, oldValue);
        }
        return removed;
    }

    private void removeLastPage(TablePage page) {
        FileIO fileIO = new FileIO(table.getFileIOThread());
        try {
            fileIO.truncateFile(table.getPath(), page.sizeInBytes());
        } catch (ExecutionException e) {
            logger.log(Level.SEVERE, "ExecutionException while truncating file for removing last page.", e);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "InterruptedException while truncating file for removing last page.", e);
            Thread.currentThread().interrupt(); // good practice to reset interrupt status
        }
        table.removeOnePage();
    }

    //==UPDATING==
    public int updateEntry(Object start, Object end, int columnIndex, int limit, List<Map.Entry<String,InnerFunctions>> updates) {
        int result = 0;
        List<BlockPointer> blockPointerList = table.findRangeIndex(start, end, columnIndex);
        boolean updateAll = limit < 0;
        int index = -1;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!updateAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            this.updateProcess(page, blockPointer, updates);
            result++;
        }
        return result;
    }
    private void updateProcess(TablePage page, BlockPointer blockPointer,  List<Map.Entry<String,InnerFunctions>> updates) {
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        Object[] values = removed.getEntry();
        values = this.applyUpdates(values, updates);
        if(!Entry.isValidEntry(values, table.getSchema())) throw new IllegalArgumentException("Unable to update, final entry is invalid.");
        Entry updatedEntry = new Entry(values,table);
        page.add(updatedEntry);
        table.insertIndex(updatedEntry, blockPointer);
    }
    private Object[] applyUpdates(Object[] values, List<Map.Entry<String,InnerFunctions>> updates){
        ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(table.getSchema().getNames()));
        for (Map.Entry<String, InnerFunctions> update : updates) {
            int index = columnNames.indexOf(update.getKey());
            if(index<0) throw new IllegalArgumentException("Invalid column to update: "+update.getKey());
            values[index] = update.getValue().apply(table.getSchema(), values, index);
        }
        return values;
    }
}
