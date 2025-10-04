package com.database.db.manager;

import java.util.List;

import com.database.db.index.BTreeInit;
import com.database.db.index.BTreeInit.BlockPointer;
import com.database.db.index.BTreeInit.PointerPair;
import com.database.db.index.Pair;
import com.database.db.page.Entry;
import com.database.db.page.IndexPage;
import com.database.db.table.Table;

public class IndexPageManager {
    private final Table table;
    public IndexPageManager(Table table){
        this.table = table;
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> BlockPointer findIndexPointer(BTreeInit<?> index, K key, BlockPointer tablePointer){
        List<Pair<K, PointerPair>> pointerList = ((BTreeInit<K>)index).search(key);
        BlockPointer result = null;
        for (Pair<K, PointerPair> pair : pointerList) {
            if(tablePointer.equals(pair.value.tablePointer())) result = pair.value.indexPointer();
        }
        return result;
    }

    public BlockPointer insert(BlockPointer pointer, Object value, int columnIndex){
        if(table.getIndexManager().getPages(columnIndex)==0)table.getIndexManager().addOnePage(columnIndex);
        IndexPage page = table.getCache().getLastIndexPage(columnIndex);
        Object[] values = {pointer, value};
        boolean isNotNullable = table.getSchema().getNotNull()[columnIndex];
        int numOfNulls = !isNotNullable? 1:0;
        Entry entry = new Entry(values, numOfNulls)
            .setBitMap(new boolean[]{true,isNotNullable});
        if(page.size() >= page.getCapacity()){
            this.table.getIndexManager().addOnePage(columnIndex);
            page = table.getCache().getLastIndexPage(columnIndex);
        }
        page.add(entry);
        table.getCache().putIndexPage(page);
        return new BlockPointer(page.getPageID(), (short)(page.size()-1));
    }

    public <K extends Comparable<? super K>> void remove(BTreeInit<K> index, PointerPair pointerPair, int columnIndex, Object[] keys, PointerPair[] values, PointerPair[] oldValues){
        BlockPointer indexPointer = pointerPair.indexPointer();
        IndexPage page = this.removalProcess(pointerPair, columnIndex);
        if (page.isLastPage() && page.size() == 1) {
            page.remove(indexPointer.RowOffset());
            table.getCache().deleteLastIndexPage(page);
            return;
        }
        this.replaceWithLast(index, page, indexPointer, columnIndex,keys,values,oldValues);
        table.getCache().putIndexPage(page);
    }
    private IndexPage removalProcess(PointerPair pointerPair, int columnIndex){
        BlockPointer indexPointer = pointerPair.indexPointer();
        IndexPage page = table.getCache().getIndexPage(pointerPair.indexPointer().BlockID(), columnIndex);
        Entry removedEntry = page.get(indexPointer.RowOffset());
        page.set(indexPointer.RowOffset(), null);
        if (!removedEntry.get(0).equals(pointerPair.tablePointer()))
            throw new IllegalArgumentException("Mismatching table pointer from removed index entry");
        return page;
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> void replaceWithLast(BTreeInit<K> index, IndexPage page, BlockPointer indexPointer, int columnIndex, Object[] keys, PointerPair[] values, PointerPair[] oldValues){
        if(page.isLastPage()){
            page.remove(indexPointer.RowOffset());
            if(indexPointer.RowOffset() != page.size()){
                Entry swapped = page.get(indexPointer.RowOffset());
                BlockPointer oldPointer = new BlockPointer(page.getPageID(),page.size());
                PointerPair oldPair = new PointerPair((BlockPointer)swapped.get(0), oldPointer);
                PointerPair newPair = new PointerPair((BlockPointer)swapped.get(0), indexPointer);
                if(index.isUnique())index.update((K)swapped.get(1), newPair);
                else index.update((K)swapped.get(1), newPair, oldPair);
                keys[columnIndex] = swapped.get(1);
                values[columnIndex] = newPair;
                oldValues[columnIndex] = oldPair;
            }
        }else{
            IndexPage lastPage = table.getCache().getLastIndexPage(columnIndex);
            int lastOffset = lastPage.size() - 1;
            Entry lastEntry = lastPage.get(lastOffset);
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short)lastOffset);
            PointerPair oldPair = new PointerPair((BlockPointer)lastEntry.get(0), oldPointer);

            lastPage.removeLast();

            page.set(indexPointer.RowOffset(), lastEntry);
            PointerPair newPair = new PointerPair((BlockPointer) lastEntry.get(0), 
                                      new BlockPointer(page.getPageID(), (short) indexPointer.RowOffset()));
            if(index.isUnique())index.update((K)lastEntry.get(1), newPair);
            else index.update((K)lastEntry.get(1), newPair, oldPair);
            keys[columnIndex] = lastEntry.get(1);
            values[columnIndex] = newPair;
            oldValues[columnIndex] = oldPair;
            if(lastPage.size() == 0){
                table.getCache().deleteLastIndexPage(lastPage);
            }else{
                table.getCache().putIndexPage(lastPage);
            }
        }
    }
    public void update(BlockPointer IndexPointer, BlockPointer newPointer, Object newValue, int columnIndex){
        if (IndexPointer == null) throw new IllegalArgumentException("IndexPointer is null");
        IndexPage page = table.getCache().getIndexPage(IndexPointer.BlockID(),columnIndex);
        int row = IndexPointer.RowOffset();
        if (row < 0 || row >= page.size()) {
            throw new IndexOutOfBoundsException("Invalid index pointer: " + row + " page size: " + page.size());
        }
        Entry updated = page.get(row);
        updated.set(0, newPointer);
        updated.set(1, newValue);
        table.getCache().putIndexPage(page);
    }
}