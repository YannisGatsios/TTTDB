package com.database.tttdb.core.index;

import java.nio.ByteBuffer;
import java.util.List;

import com.database.tttdb.core.index.btree.BPlusTree;
import com.database.tttdb.core.index.hashmap.HashIndex;
import com.database.tttdb.core.index.redBlackTreeIndex.RedBlackTreeIndex;
import com.database.tttdb.core.index.skiplist.SkipListIndex;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.page.IndexPage;
import com.database.tttdb.core.table.Table;

/**
 * B+Tree-based index implementation.
 * Maps keys to {@link PointerPair} objects that link table and index locations.
 * Acts as an adapter between the generic {@link Index} interface and
 * the {@link BPlusTree} structure.
 */
public class IndexInit<K extends Comparable<? super K>> implements Index<K,IndexInit.PointerPair>{
    protected int columnIndex;
    private final Index<K,IndexInit.PointerPair> index;
    public record BlockPointer(int BlockID, short RowOffset){
        public static final int BYTES = 6;
        public byte[] toBytes(){
            ByteBuffer buffer = ByteBuffer.allocate(6);
            buffer.putInt(BlockID);
            buffer.putShort(RowOffset);
            return buffer.array();
        }
        public static BlockPointer fromBytes(ByteBuffer buffer) {
            int BlockID = buffer.getInt();
            short RowOffset = buffer.getShort();
            return new BlockPointer(BlockID, RowOffset);
        }
    }
    public record PointerPair(BlockPointer tablePointer, BlockPointer indexPointer) {} 

    public IndexInit(int order){
        this.index = new RedBlackTreeIndex<>();
    }
    @SuppressWarnings("unchecked")
    public IndexInit<K> initialize(Table table){
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return this;
        IndexPage page;
        Entry[] list;
        for(int i = 0;i < numberOfPages;i++){
            page = table.getCache().getIndexPage(i, columnIndex);
            list = page.getAll();
            for (short j = 0; j < page.size(); j++) {
                Entry entry = list[j];
                K key = (K) entry.get(1);
                BlockPointer tablePointer = (BlockPointer)entry.get(0);
                BlockPointer indexPointer = new BlockPointer(i, j);
                PointerPair value = new PointerPair(tablePointer, indexPointer);
                this.insert(key, value);
            }
        }
        return this;
    }

    public void insert(K key, PointerPair value) {
        this.index.insert(key, value);
    }
    public void remove(K key, PointerPair value) {
        this.index.remove(key, value);
    }
    public List<Pair<K, PointerPair>> search(K key) {
        return this.index.search(key);
    }
    public List<Pair<K, PointerPair>> rangeSearch(K fromKey, K toKey) {
        return this.index.rangeSearch(fromKey, toKey);
    }
    public boolean isKey(K key) {
        return this.index.isKey(key);
    }
    public void update(K key, PointerPair newValue) {
        this.index.update(key, newValue);
    }
    public void update(K key, PointerPair newValue, PointerPair oldValue) {
        this.index.update(key, newValue, oldValue);
    }
    
    public void setUnique(boolean isUnique) {
        this.index.setUnique(isUnique);
    }
    public void setNullable(boolean isNullable) {
        this.index.setNullable(isNullable);
    }
    public boolean isUnique() {
        return this.index.isUnique();
    }
    public boolean isNullable() {
        return this.index.isNullable();
    }
    public long size() {
        return index.size();
    }
    public K getMax() {
        return this.index.getMax();
    }
    public void clear(){
        this.index.clear();
    }

    public void setColumnIndex(int columnIndex){this.columnIndex = columnIndex;}
    public int getColumnIndex(){return this.columnIndex;}
}