package com.database.db.index;

import java.nio.ByteBuffer;

import com.database.db.page.Entry;
import com.database.db.page.IndexPage;
import com.database.db.table.Table;

public class BTreeSerialization<K extends Comparable<? super K>> extends BPlusTree<K,BTreeSerialization.PointerPair>{
    protected int columnIndex;
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

    public BTreeSerialization(int order){
        super(order);
    }
    @SuppressWarnings("unchecked")
    public BTreeSerialization<K> initialize(Table table){
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return this;
        IndexPage page;
        Entry[] list;
        for(int i = 0;i < numberOfPages;i++){
            page = table.getCache().indexCaches[columnIndex].get(i);
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

    public void setColumnIndex(int columnIndex){this.columnIndex = columnIndex;}
    public int getColumnIndex(){return this.columnIndex;}
}