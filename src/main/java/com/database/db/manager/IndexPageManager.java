package com.database.db.manager;

import java.util.List;

import com.database.db.index.BTreeSerialization;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.index.BTreeSerialization.PointerPair;
import com.database.db.page.Entry;
import com.database.db.page.IndexCache;
import com.database.db.page.IndexPage;
import com.database.db.table.Table;

public class IndexPageManager {
    private Table table;
    private IndexCache[] cache;
    public IndexPageManager(Table table){
        this.table = table;
        this.cache = table.getCache().indexCaches;
    }

    public <K extends Comparable<? super K>> BlockPointer findIndexPointer(BTreeSerialization<?> index, K key, BlockPointer tablePointer){
        List<PointerPair> pointerList = ((BTreeSerialization<K>)index).search(key);
        BlockPointer result = null;
        for (PointerPair pointerPair : pointerList) {
            if(tablePointer.equals(pointerPair.tablePointer())) result = pointerPair.indexPointer();
        }
        return result;
    }

    public BlockPointer insert(BlockPointer pointer, Object value, int columnIndex){
        if(table.getIndexManager().getPages(columnIndex)==0)table.getIndexManager().addOnePage(columnIndex);
        IndexPage page = this.cache[columnIndex].getLast();
        Object[] values = {pointer, value};
        boolean isNotNullable = table.getSchema().getNotNull()[columnIndex];
        int numOfNulls = !isNotNullable? 1:0;
        Entry entry = new Entry(values, numOfNulls)
            .setBitMap(new boolean[]{true,isNotNullable});
        if(page.size() >= page.getCapacity()){
            this.table.getIndexManager().addOnePage(columnIndex);
            page = this.cache[columnIndex].getLast();
        }
        page.add(entry);
        this.cache[columnIndex].put(page);
        return new BlockPointer(page.getPageID(), (short)(page.size()-1));
    }
    public record RemoveResult(Entry swapedEntry, short previusPosition, Entry replacedEntry, BlockPointer lasteEntryPoionter){}
    public RemoveResult remove(PointerPair pairPointer, int columnIndex){
        IndexPage page = this.cache[columnIndex].get(pairPointer.indexPointer().BlockID());

        int removedPos = pairPointer.indexPointer().RowOffset();
        Entry removedEntry = page.remove(removedPos);

        if (!removedEntry.get(0).equals(pairPointer.tablePointer()))
            throw new IllegalArgumentException("Missmaching table pointer from removed index entry");
        Entry swappedEntry = null;
        short previousPosition = -1;

        if (removedPos < page.size()) {
            swappedEntry = page.get(removedPos);
            previousPosition = (short) page.size();
        }

        Entry replacedEntry = null;
        BlockPointer lastEntryPointer = null;
        if (!page.isLastPage()) {
            IndexPage lastPage = this.cache[columnIndex].getLast();
            lastEntryPointer = new BlockPointer(lastPage.getPageID(), (short)(lastPage.size()-1));
            replacedEntry = lastPage.removeLast();
            page.add(replacedEntry);
            this.cache[columnIndex].put(lastPage);

            if (lastPage.size() == 0) {
                this.cache[columnIndex].deleteLastPage(lastPage);
            }
        }

        this.cache[columnIndex].put(page);

        if (page.size() == 0) {
            this.cache[columnIndex].deleteLastPage(page);
        }
        return new RemoveResult(swappedEntry, previousPosition, replacedEntry, lastEntryPointer);
    }
    public void update(BlockPointer IndexPointer, BlockPointer newPointer, Object newValue, int columnIndex){
        if (IndexPointer == null) throw new IllegalArgumentException("IndexPointer is null");
        IndexPage page = this.cache[columnIndex].get(IndexPointer.BlockID());
        int row = IndexPointer.RowOffset();
        if (row < 0 || row >= page.size()) {
            throw new IndexOutOfBoundsException("Invalid index pointer: " + row + " page size: " + page.size());
        }
        Entry updated = page.get(row);
        updated.set(0, newPointer);
        updated.set(1, newValue);
        this.cache[columnIndex].put(page);
    }
}