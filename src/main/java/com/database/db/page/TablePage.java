package com.database.db.page;

import java.nio.ByteBuffer;

import com.database.db.table.DataType;
import com.database.db.table.Table;

public class TablePage extends Page{

    private final Table table;
    private final String filePath;

    public TablePage(int PageID, Table table) {
        super(PageID, TablePage.sizeOfEntry(table));
        this.table = table;
        this.filePath = table.getPath();
    }

    public static int sizeOfEntry(Table table){
        int result = (table.getSchema().numNullables()+7)/8;
        DataType[] columnTypes = table.getSchema().getTypes();
        int[] columnSizes = table.getSchema().getSizes();
        for (int i = 0;i<columnTypes.length;i++) {
            int size = columnTypes[i].getSize();
            if(size == -1) size = columnSizes[i]+2;
            result += size;
        }
        return result;
    }

    public TablePage deepCopy() {
        TablePage clone = new TablePage(this.getPageID(), this.table);
        for (int i = 0; i < this.size(); i++) {
            clone.add(this.get(i).deepCopy());
        }
        // copy bookkeeping fields
        clone.setDirty(this.isDirty());
        return clone;
    }

    public byte[] toBytes() {
        ByteBuffer combinedArray = ByteBuffer.allocate(this.sizeInBytes());
        Page.headerToBytes(this,combinedArray);
        // Add entries
        Entry[] entryList = this.getAll();
        for (int i = 0;i<this.size();i++) {
            Entry entry = entryList[i];
            combinedArray.put(entry.toBytes(table));
        }
        combinedArray.flip();
        // Return the underlying byte array
        return combinedArray.array();
    }

    public void fromBytes(byte[] bufferData) {
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("PageID: "+pageID+" Buffer data cannot be null or empty.");
        if (bufferData.length%4096 != 0) throw new IllegalArgumentException("PageID: "+pageID+" Buffer data must be a modulo of a Blocks Size(4096 BYTES) you gave : "+bufferData.length);
        ByteBuffer buffer = ByteBuffer.wrap(bufferData);
        //Reading The Header
        HeaderValues result = Page.headerFromBytes(buffer, this);
        //Reading The Entries
        int entrySize = TablePage.sizeOfEntry(table);
        for(int i = 0; i < result.numOfEntries(); i++){
            ByteBuffer slice = buffer.slice(); // view starting at current position
            slice.limit(entrySize); // limit to just one entry
            Entry newEntry = Entry.fromBytes(slice, table);
            buffer.position(buffer.position() + entrySize);
            this.add(newEntry);
        }
        if(result.spaceInUse() != this.getSpaceInUse()){
            throw new IllegalArgumentException("PageID: "+pageID+" Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ result.spaceInUse()+" BlockID:"+this.getPageID());
        }
        if(result.numOfEntries() != this.size()) throw new IllegalArgumentException("PageID: "+pageID+" Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public boolean isLastPage() { return (this.getPageID() == table.getPages()-1); }
    public String getFilePath() { return this.filePath; }
}
