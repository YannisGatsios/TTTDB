package com.database.db.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class TablePage<K extends Comparable<K>> extends Page<K>{

    public TablePage(int PageID, Table<K> table) throws InterruptedException, ExecutionException, IOException {
        super(PageID, table);
        String tablePath = table.getTablePath();
        FileIOThread fileIOThread = table.getFileIOThread();
        FileIO FileIO = new FileIO(fileIOThread);
        byte[] pageBuffer = FileIO.readPage(tablePath, this.getPagePos(), this.sizeInBytes());
        if (pageBuffer == null || pageBuffer.length == 0) return; //Checking if Page we read is empty.
        this.bufferToPage(pageBuffer, table);
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
        for (Entry<K> entryObj : this.getAll()) {
            ArrayList<Object> entry = entryObj.getEntry();
            for (Object element : entry) {
                switch (element.getClass().getSimpleName()) {
                    case "Integer":
                        buffer.putInt((Integer) element); // Add integer elements as 4 bytes
                        break;
                    case "byte[]":
                        byte[] byteArray = (byte[]) element;
                        buffer.putShort((short) byteArray.length); // Store length of byte array
                        buffer.put(byteArray); // Add byte array elements
                        break;
                    case "String":
                        byte[] strBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
                        buffer.putShort((short) strBytes.length); // Store length of string
                        buffer.put(strBytes); // Then the string bytes
                        break;
                    default:
                        throw new IOException("Unsupported data type in entry.");
                }
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

    public void bufferToPage(byte[] bufferData, Table<K> table) throws IOException{
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
            
            for (String type : table.getSchema().getColumnTypes()) {
                switch (type) {
                    case "String":
                        short size = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex + 2)).getShort();
                        startIndex += 2;
                        entry.add(new String(Arrays.copyOfRange(bufferData, startIndex, startIndex + size), StandardCharsets.UTF_8));
                        startIndex += size;
                        break;
                    case "Integer":
                        entry.add(ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex + 4)).getInt());
                        startIndex += 4;
                        break;
                    case "Byte":
                        int byteSize = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex + 2)).getShort();
                        startIndex += 2;
                        byte[] data = Arrays.copyOfRange(bufferData, startIndex, startIndex + byteSize);
                        entry.add(data);
                        startIndex += byteSize;
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected type: " + type);
                }
            }
            Entry<K> newEntry = new Entry<>(entry, table.getPrimaryKeyMaxSize());
            newEntry.setID(table.getPrimaryKeyColumnIndex());
            this.add(newEntry);
        }
        if(spaceInUse != this.getSpaceInUse()){
            throw new IOException("Mismatch between expected and actual space in use for Page. newBlock:"+this.getSpaceInUse()+" oldBlock:"+ spaceInUse+" BlockID:"+this.getPageID());
        }
        if(numOfEntries != this.size()) throw new IOException("Mismatch between expected and actual numOfEntries and Page.size().");
    }

    public void write(Table<K> table) throws IOException {
        FileIO fileIO = new FileIO(table.getFileIOThread());
        fileIO.writePage(table.getTablePath(), this.toBuffer(), this.getPagePos());
    }
}
