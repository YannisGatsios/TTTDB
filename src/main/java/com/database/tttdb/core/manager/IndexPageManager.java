package com.database.tttdb.core.manager;

import java.util.List;

import com.database.tttdb.core.index.IndexInit;
import com.database.tttdb.core.index.Pair;
import com.database.tttdb.core.index.IndexInit.BlockPointer;
import com.database.tttdb.core.index.IndexInit.PointerPair;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.page.IndexPage;
import com.database.tttdb.core.table.Table;
/**
 * Manages on-disk and cached pages for secondary/primary index structures.
 * Provides pointer lookup, index-entry insert/remove with compaction,
 * and in-place index-entry updates.
 *
 * Invariants:
 * - Each index entry is a 2-field {@link Entry}: [0]=table BlockPointer, [1]=key.
 * - Column index selects the index file/segment.
 * - Cache writes are persisted via {@code table.getCache().putIndexPage(...)}.
 */
public class IndexPageManager {
    private final Table table;
    /**
     * @param table owning table. Must be non-null and initialized.
     */
    public IndexPageManager(Table table){
        this.table = table;
    }

    /**
     * Finds the index-page pointer that corresponds to a specific table-row pointer for a given key.
     *
     * @param index index to search
     * @param key key to probe
     * @param tablePointer table-row pointer to match
     * @param <K> key type
     * @return the index {@link BlockPointer} if found, otherwise {@code null}
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> BlockPointer findIndexPointer(IndexInit<?> index, K key, BlockPointer tablePointer){
        List<Pair<K, PointerPair>> pointerList = ((IndexInit<K>)index).search(key);
        BlockPointer result = null;
        for (Pair<K, PointerPair> pair : pointerList) {
            if(tablePointer.equals(pair.value.tablePointer())) result = pair.value.indexPointer();
        }
        return result;
    }

    /**
     * Appends a new index entry [tablePointer, value] to the last index page for the column,
     * growing pages if required.
     *
     * @param pointer table-row pointer
     * @param value key value to store (nullable obeys schema.notNull[columnIndex])
     * @param columnIndex index column id
     * @return {@link BlockPointer} to the new index entry
     */
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

    /**
     * Removes an index entry and compacts by swapping the last entry into the hole
     * when needed. Populates arrays describing any swap so callers can snapshot an
     * UPDATE operation for rollback.
     *
     * @param index logical index to update after swap
     * @param pointerPair pair of (table, index) pointers identifying the entry to remove
     * @param columnIndex index column id
     * @param keys out: swapped key (if a swap occurred), else null
     * @param values out: new PointerPair of swapped entry (if swap), else null
     * @param oldValues out: old PointerPair location for swapped entry (if swap), else null
     * @param <K> key type
     * @return true if a swap occurred and outputs were populated, false if no swap
     */
    public <K extends Comparable<? super K>> boolean remove(IndexInit<K> index, PointerPair pointerPair, int columnIndex, Object[] keys, PointerPair[] values, PointerPair[] oldValues){
        BlockPointer indexPointer = pointerPair.indexPointer();
        IndexPage page = this.removalProcess(pointerPair, columnIndex);
        if (page.isLastPage() && page.size() == 1) {
            page.remove(indexPointer.RowOffset());
            table.getCache().deleteLastIndexPage(page);
            return false;
        }
        boolean changed = this.replaceWithLast(index, page, indexPointer, columnIndex,keys,values,oldValues);
        table.getCache().putIndexPage(page);
        return changed;
    }
    /**
     * Marks the slot as empty and validates that the removed entry matches the expected table pointer.
     *
     * @param pointerPair identifies the index entry to remove
     * @param columnIndex index column id
     * @return the loaded {@link IndexPage} with the hole created
     * @throws IllegalArgumentException if the table pointer does not match
     */
    private IndexPage removalProcess(PointerPair pointerPair, int columnIndex){
        BlockPointer indexPointer = pointerPair.indexPointer();
        IndexPage page = table.getCache().getIndexPage(pointerPair.indexPointer().BlockID(), columnIndex);
        Entry removedEntry = page.get(indexPointer.RowOffset());
        page.set(indexPointer.RowOffset(), null);
        if (!removedEntry.get(0).equals(pointerPair.tablePointer()))
            throw new IllegalArgumentException("Mismatching table pointer from removed index entry");
        return page;
    }
    /**
     * Compacts the page by moving a candidate entry into the hole and updates the logical index.
     * Writes swap metadata into the provided arrays for rollback.
     *
     * Cases:
     * - Last page: if the hole is not at the tail, swap with last entry on the same page.
     * - Non-last page: pull the last entry of the last page into the hole and shrink last page.
     *
     * @param index logical index to reflect new/old pointer for the swapped key
     * @param page page containing the hole
     * @param indexPointer location of the hole
     * @param columnIndex index column id
     * @param keys out key written when a swap occurs
     * @param values out new PointerPair for swapped entry
     * @param oldValues out old PointerPair for swapped entry
     * @param <K> key type
     * @return true if a swap occurred, false otherwise
     */
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> boolean replaceWithLast(IndexInit<K> index, IndexPage page, BlockPointer indexPointer, int columnIndex, Object[] keys, PointerPair[] values, PointerPair[] oldValues){
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
                return true;
            }
            return false;
        }
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
        return true;
    }
    /**
     * Updates an existing index entry in place.
     *
     * @param IndexPointer index entry location to update
     * @param newPointer new table-row pointer
     * @param newValue new key value to store
     * @param columnIndex index column id
     * @throws IllegalArgumentException if {@code IndexPointer} is null
     * @throws IndexOutOfBoundsException if the row offset is invalid for the page
     */
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