package com.database.db.page;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.database.db.table.DataType;
import com.database.db.table.Table;

public class TablePage extends Page{

    private Table table;

    public TablePage(int PageID, Table table) {
        super(PageID, TablePage.sizeOfEntry(table));
        this.table = table;
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

    public byte[] toBytes() throws IOException {
        ByteBuffer headBuffer = ByteBuffer.allocate(this.sizeOfHeader());
        ByteBuffer bodyBuffer = ByteBuffer.allocate(this.sizeOfEntries());

        // Add primitive fields
        headBuffer.putInt(this.getPageID()); // Serialize pageID as 4 bytes (int)
        headBuffer.putShort(this.size()); // Serialize numOfEntries as 2 bytes (short)
        headBuffer.putInt(this.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        headBuffer.flip();

        // Add entries
        Entry[] entryList = this.getAll();
        for (int i = 0;i<this.size();i++) {
            Entry entry = entryList[i];
            bodyBuffer.put(entry.toBytes(table));
        }
        bodyBuffer.flip();

        // Get the remaining bytes from buffer1 and buffer2 into the combinedArray
        ByteBuffer combinedArray = ByteBuffer.allocate(this.sizeInBytes());
        combinedArray.put(headBuffer.array());
        combinedArray.put(bodyBuffer.array());
        combinedArray.flip();
        // Return the underlying byte array
        return combinedArray.array();
    }

    public void fromBytes(byte[] bufferData) {
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        if (bufferData.length%4096 != 0) throw new IllegalArgumentException("Buffer data must be a modulo of a Blocks Size(4096 BYTES) you gave : "+bufferData.length);
        ByteBuffer buffer = ByteBuffer.wrap(bufferData);
        //Reading The Header
        int pageID = buffer.getInt();
        short numOfEntries = buffer.getShort();
        int spaceInUse = buffer.getInt();
        this.setPageID(pageID);
        //Reading The Entries
        int entrySize = TablePage.sizeOfEntry(table);
        for(int i = 0; i < numOfEntries; i++){
            ByteBuffer slice = buffer.slice(); // view starting at current position
            slice.limit(entrySize); // limit to just one entry
            Entry newEntry = Entry.fromBytes(slice, table);
            buffer.position(buffer.position() + entrySize);
            this.add(newEntry);
        }
        if(spaceInUse != this.getSpaceInUse()){
            throw new IllegalArgumentException("Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ spaceInUse+" BlockID:"+this.getPageID());
        }
        if(numOfEntries != this.size()) throw new IllegalArgumentException("Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public boolean isLastPage(){return (this.getPageID() == table.getPages()-1);}
}
