package com.database.db.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class TablePage extends Page{

    private Table table;

    public TablePage(int PageID, Table table) throws InterruptedException, ExecutionException, IOException {
        super(PageID, table);
        this.table = table;
        String tablePath = table.getPath();
        FileIOThread fileIOThread = table.getFileIOThread();
        FileIO FileIO = new FileIO(fileIOThread);
        byte[] pageBuffer = FileIO.readPage(tablePath, this.getPagePos(), this.sizeInBytes());
        if (pageBuffer == null || pageBuffer.length == 0){//Checking if Page we read is empty.
            this.write(table);
            return;
        }
        this.fromBytes(pageBuffer);
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

    public void fromBytes(byte[] bufferData) throws IOException{
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        if (bufferData.length%4096 != 0) throw new IllegalArgumentException("Buffer data must be a modulo of a Blocks Size(4096 BYTES) you gave : "+bufferData.length);
        ByteBuffer buffer = ByteBuffer.wrap(bufferData);
        //Reading The Header
        int pageID = buffer.getInt();
        short numOfEntries = buffer.getShort();
        int spaceInUse = buffer.getInt();
        this.setPageID(pageID);
        //Reading The Entries
        int entrySize = table.getSizeOfEntry();
        for(int i = 0; i < numOfEntries; i++){
            ByteBuffer slice = buffer.slice(); // view starting at current position
            slice.limit(entrySize); // limit to just one entry
            Entry newEntry = Entry.fromBytes(slice, table);
            buffer.position(buffer.position() + entrySize);
            this.add(newEntry);
        }
        if(spaceInUse != this.getSpaceInUse()){
            throw new IOException("Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ spaceInUse+" BlockID:"+this.getPageID());
        }
        if(numOfEntries != this.size()) throw new IOException("Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public void write(Table table) throws IOException {
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writePage(table.getPath(), this.toBytes(), this.getPagePos());
    }

    public boolean isLastPage(){return (this.getPageID() == table.getPages()-1);}
}
