package com.database.db.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.table.Entry;
import com.database.db.table.Table;
import com.database.db.table.Type;
import com.database.db.table.Type.DeserializationResult;

public class TablePage extends Page{

    private Table table;

    public TablePage(int PageID, Table table) throws InterruptedException, ExecutionException, IOException {
        super(PageID, table);
        this.table = table;
        String tablePath = table.getPath();
        FileIOThread fileIOThread = table.getFileIOThread();
        FileIO FileIO = new FileIO(fileIOThread);
        byte[] pageBuffer = FileIO.readPage(tablePath, this.getPagePos(), this.sizeInBytes());
        if (pageBuffer == null || pageBuffer.length == 0) return; //Checking if Page we read is empty.
        this.bufferToPage(pageBuffer);
    }

    public byte[] toBuffer() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(this.sizeOfEntries());
        ByteBuffer headBuffer = ByteBuffer.allocate(this.sizeOfHeader());

        // Add primitive fields
        headBuffer.putInt(this.getPageID()); // Serialize pageID as 4 bytes (int)
        headBuffer.putShort(this.size()); // Serialize numOfEntries as 2 bytes (short)
        headBuffer.putInt(this.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        headBuffer.flip();

        // Add entries
        for (Entry entry : this.getAll()) {
            ArrayList<Object> values = entry.getEntry();
            for (int i = 0;i<values.size();i++) {
                Type elem = this.table.getSchema().getTypes()[i];
                buffer.put(elem.toBytes(values.get(i)));
            }
        }
        buffer.flip();

        ByteBuffer combinedArray = ByteBuffer.allocate(this.sizeInBytes());
        // Get the remaining bytes from buffer1 and buffer2 into the combinedArray
        combinedArray.put(headBuffer.array());
        combinedArray.put(buffer.array());
        combinedArray.flip();
        // Return the underlying byte array
        return combinedArray.array();
    }

    public void bufferToPage(byte[] bufferData) throws IOException{
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        if (bufferData.length%4096 != 0) throw new IllegalArgumentException("Buffer data must be a modulo of a Blocks Size(4096 BYTES) you gave : "+bufferData.length);
        if (table == null || table.getSchema() == null) throw new IllegalArgumentException("Table or table schema cannot be null.");

        //Reading The page ID.
        int pageID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        this.setPageID(pageID);
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //Initializing New Empty Page.

        //Reading The Number Of Entries.
        short numOfEntries = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 2)).getShort();
        bufferData = Arrays.copyOfRange(bufferData, 2, bufferData.length);

        //Reading The Space In Use Of The Page.
        int spaceInUse = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //Reading The Actual Entries one by one based on the tables schema configuration.
        int startIndex = 0;
        for(int i = 0; i < numOfEntries; i++){
            ArrayList<Object> entry = new ArrayList<>();
            for (Type type : table.getSchema().getTypes()) {
                DeserializationResult result = type.fromBytes(bufferData, startIndex);
                Object element = result.valueObject();
                startIndex = result.nextIndex();
                entry.add(element);
            }
            Entry newEntry = new Entry(entry, table.getPrimaryKeyMaxSize());
            this.add(newEntry);
        }
        if(spaceInUse != this.getSpaceInUse()){
            throw new IOException("Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ spaceInUse+" BlockID:"+this.getPageID());
        }
        if(numOfEntries != this.size()) throw new IOException("Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public void write(Table table) throws IOException {
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writePage(table.getPath(), this.toBuffer(), this.getPagePos());
    }
}
