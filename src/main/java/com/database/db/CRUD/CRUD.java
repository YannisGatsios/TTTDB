package com.database.db.CRUD;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class CRUD {
    private FileIO fileIO;
    public CRUD(FileIOThread fileIOThread){
        this.fileIO = new FileIO(fileIOThread);
    }
    //==SELECTING==
    public List<Entry> selectEntry(Table table, Object start,Object end, int columnIndex) throws IOException, ExecutionException, InterruptedException {
        return this.selectEntry(table, start, end, columnIndex, -1);
    }
    public List<Entry> selectEntry(Table table, Object start,Object end, int columnIndex,int limit) throws IOException, ExecutionException, InterruptedException {
        List<Entry> result = new ArrayList<>();
        int index = -1;
        List<BlockPointer> blockPointerList = table.findRangeIndex(start, end, columnIndex);
        boolean selectAll = limit < 0;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!selectAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            result.add(page.get(blockPointer.RowOffset()));
        }
        return result;
    }
    //==INSERTION==
    public void insertEntry(Table table, Entry entry) throws IOException, ExecutionException, InterruptedException,Exception {
        if(Entry.isValidEntry(entry.getEntry(),table.getSchema()))
        if(table.getPages()==0)table.addOnePage();
        TablePage page = table.getCache().getLast();
        if(page.size() < page.getCapacity()){
            this.insertionProcess(table, entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().getLast();
        this.insertionProcess(table, entry, page);
    }
    private void insertionProcess(Table table, Entry entry, TablePage page) throws IOException{
        page.add(entry);
        table.insertIndex(entry, new BlockPointer(page.getPageID(), (short)(page.size()-1)));
    }
    //==DELETION==
    public List<Entry> deleteEntry(Table table, Object start, Object end, int columnIndex) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        return this.deleteEntry(table, start, end, columnIndex, -1);
    }
    public List<Entry> deleteEntry(Table table, Object start, Object end, int columnIndex, int limit) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        ArrayList<Entry> result = new ArrayList<>();
        int index = -1;
        List<BlockPointer> blockPointerList = table.findRangeIndex(start,end,columnIndex);
        boolean deleteAll = limit < 0;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!deleteAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            if (page == null) continue;
            result.add(this.deletionProcess(table, page, blockPointer));
            if (page.isLastPage() && page.size() == 0) {
                fileIO.deleteLastPage(table.getPath(), page.sizeInBytes());
                table.removeOnePage();
                continue;
            }
            if(page.isLastPage()) continue;
            TablePage lastPage = table.getCache().getLast();
            Entry lastEntry = lastPage.removeLast();
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short)(lastPage.size()));
            page.add(lastEntry);
            table.updateIndex(lastEntry, blockPointer, oldPointer);
            if (lastPage.size() == 0) {
                fileIO.deleteLastPage(table.getPath(), lastPage.sizeInBytes());
                table.removeOnePage();
            }
        }
        return result;
    }
    private Entry deletionProcess(Table table, TablePage page, BlockPointer blockPointer) throws IOException, ExecutionException, InterruptedException {
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        if(page.size() > 0 && blockPointer.RowOffset() != page.size()){
            Entry swapped = page.get(blockPointer.RowOffset());//On removal last entry gets the place of the removed one
            BlockPointer oldValue = new BlockPointer(page.getPageID(), page.size());
            table.updateIndex(swapped, blockPointer, oldValue);
        }
        return removed;
    }

    //==UPDATING==
    public int update(Table table, Object start, Object end, int columnIndex, int limit, Map<Integer, Object> updates){
        int result = 0;
        List<BlockPointer> blockPointerList = table.findRangeIndex(start, end, columnIndex);
        boolean updateAll = limit < 0;
        int index = -1;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!updateAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            this.updateProcess(table, page, blockPointer, updates);
            result++;
        }
        return result;
    }
    private void updateProcess(Table table, TablePage page, BlockPointer blockPointer,  Map<Integer, Object> updates){
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        Object[] values = removed.getEntry();
        for (Map.Entry<Integer, Object> change : updates.entrySet()) {
            values[change.getKey()] = change.getValue();
        }
        Entry updatedEntry = new Entry(values,table);
        page.add(updatedEntry);
        table.insertIndex(updatedEntry, blockPointer);
    }
}
