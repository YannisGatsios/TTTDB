package com.database.db.core.page;

import java.nio.ByteBuffer;

import com.database.db.core.index.IndexInit.BlockPointer;
import com.database.db.core.table.DataType;
import com.database.db.core.table.Table;

public class IndexPage extends Page{

    private final Table table;
    private final int columnIndex;
    private final String filePath;

    public IndexPage(int PageID, Table table, int columnIndex) {
        super(PageID, IndexPage.sizeOfEntry(table, columnIndex));
        this.table = table;
        this.columnIndex = columnIndex;
        this.filePath = table.getIndexPath(columnIndex);
    }

    public static int sizeOfEntry(Table table, int columnIndex){
        DataType type = table.getSchema().getTypes()[columnIndex];
        boolean isNullable = !table.getSchema().getNotNull()[columnIndex];
        int result = isNullable? 1:0;//Allocate 1 byte for the Null bitmap
        result += BlockPointer.BYTES;
        int size = type.getSize();
        result += size==-1? table.getSchema().getSizes()[columnIndex]+2 : size;
        return result;
    }

    public IndexPage deepCopy() {
        IndexPage clone = new IndexPage(this.getPageID(), this.table, this.getColumnIndex());
        for (int i = 0; i < this.size(); i++) {
            clone.add(this.get(i).deepCopy());
        }
        clone.setDirty(this.isDirty());
        return clone;
    }

    public byte[] toBytes() {
        ByteBuffer combinedArray = ByteBuffer.allocate(this.sizeInBytes());
        Page.headerToBytes(this, combinedArray);
        // Add entries
        Entry[] entryList = this.getAll();
        for (int i = 0;i<this.size();i++) {
            Entry entry = entryList[i];
            combinedArray.put(entry.toBytes(table, columnIndex));
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
        int entrySize = IndexPage.sizeOfEntry(table, columnIndex);
        for(int i = 0; i < result.numOfEntries(); i++){
            ByteBuffer slice = buffer.slice(); // view starting at current position
            slice.limit(entrySize); // limit to just one entry
            Entry newEntry = Entry.fromBytes(slice, table, columnIndex);
            buffer.position(buffer.position() + entrySize);
            this.add(newEntry);
        }
        if(result.spaceInUse() != this.getSpaceInUse()){
            throw new IllegalArgumentException("PageID: "+pageID+" Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ result.numOfEntries()+" BlockID:"+this.getPageID());
        }
        if(result.numOfEntries() != this.size()) throw new IllegalArgumentException("PageID: "+pageID+" Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public int getColumnIndex() { return this.columnIndex; }
    public boolean isLastPage() { return (this.getPageID() == table.getIndexManager().getPages(columnIndex)-1); }
    public String getFilePath() { return this.filePath; }
}