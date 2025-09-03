package com.database.db.page;

import java.nio.ByteBuffer;

import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.table.DataType;
import com.database.db.table.Table;

public class IndexPage extends Page{

    private Table table;
    private int columnIndex;

    public IndexPage(int PageID, Table table, int columnIndex) {
        super(PageID, IndexPage.sizeOfEntry(table, columnIndex));
        this.table = table;
        this.columnIndex = columnIndex;
    }

    public static int sizeOfEntry(Table table, int columnIndex){
        DataType type = table.getSchema().getTypes()[columnIndex];
        boolean isNullable = !table.getSchema().getNotNull()[columnIndex];
        int result = isNullable? 1:0;//Alocate 1 byte for the Null bitmap
        result += BlockPointer.BYTES;
        int size = type.getSize();
        result += size==-1? table.getSchema().getSizes()[columnIndex]+2 : size;
        return result;
    }

    public byte[] toBytes() {
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
            bodyBuffer.put(entry.toBytes(table, columnIndex));
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
        int entrySize = IndexPage.sizeOfEntry(table, columnIndex);
        for(int i = 0; i < numOfEntries; i++){
            ByteBuffer slice = buffer.slice(); // view starting at current position
            slice.limit(entrySize); // limit to just one entry
            Entry newEntry = Entry.fromBytes(slice, table, columnIndex);
            buffer.position(buffer.position() + entrySize);
            this.add(newEntry);
        }
        if(spaceInUse != this.getSpaceInUse()){
            throw new IllegalArgumentException("Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ spaceInUse+" BlockID:"+this.getPageID());
        }
        if(numOfEntries != this.size()) throw new IllegalArgumentException("Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public int getColumnIndex(){return this.columnIndex;}
    public boolean isLastPage(){return (this.getPageID() == table.getIndexManager().getPages(columnIndex)-1);}
}