package com.database.db.page;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import com.database.db.Table;
import com.database.db.Entry;

public class PageManager<K> {
    public void writePage(String path, byte[] pageBuffer, int pagePosition){
        if (pageBuffer == null || pageBuffer.length == 0) throw new IllegalArgumentException("Page buffer cannot be null or empty. When Writing a Page.");
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("Path cannot be null or empty. When Writing a Page.");
        try {
            RandomAccessFile raf = new RandomAccessFile(path, "rw");
            raf.seek(pagePosition);
            raf.write(pageBuffer, 0, pageBuffer.length);
            raf.close();
        } catch (IOException e) {
            //LOGGING
            System.err.println("Error writing to file: " + path);
            System.err.println("Page position: " + pagePosition);
            e.printStackTrace();

            throw new RuntimeException("Failed to write page to file: " + path, e);
        }
    }

    public byte[] readPage(String path, int pagePosition, int pageMaxSize){
        if (path == null || path.isEmpty()) throw new IllegalArgumentException("Path cannot be null or empty when trying to read Page.");
        if (pageMaxSize <= 0) throw new IllegalArgumentException("Page max size must be greater than zero you gave : "+pageMaxSize);
        if (pageMaxSize%4096 != 0) throw new IllegalArgumentException("Page size must be a modulo of a Blocks Size(4096 BYTES) value you gave is : "+pageMaxSize);
        
        byte[] buffer = new byte[pageMaxSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(path, "r");
            raf.seek(pagePosition);
            int bytesRead = raf.read(buffer, 0, pageMaxSize);
            if (bytesRead == -1) {
                // File is empty or end of file is reached
                raf.close();
                return null; // Return an empty byte array
            }
            if (bytesRead < pageMaxSize) {
                byte[] actualBytes = new byte[bytesRead];
                System.arraycopy(buffer, 0, actualBytes, 0, bytesRead);
                buffer = actualBytes;
            }
            raf.close();
        } catch (FileNotFoundException e) {
            System.err.println("File not found(Can not read Page): " + path);
            throw new RuntimeException("File not found at specified path: " + path, e);
        } catch (IOException e) {
            System.err.println("Error reading from file: " + path);
            throw new RuntimeException("Failed to read page from file: " + path, e);
        }
        return buffer;
    }

    public void deleteLastPage(String path, int pageSize){
        try (RandomAccessFile file = new RandomAccessFile(path, "rw");
                FileChannel fileChannel = file.getChannel()) {

            // Get the current size of the file
            long fileSize = fileChannel.size();
            // Calculate the new size (truncate last 100 bytes)
            long newSize = fileSize - pageSize;
            if (newSize < 0) {
                throw new IOException("File is smaller than 100 bytes; cannot truncate.");
            }
            // Truncate the file
            fileChannel.truncate(newSize);
            System.out.println("======== Last "+pageSize+" bytes truncated. New file size: " + newSize + " bytes.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] pageToBuffer(Page<K> page) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(page.sizeOfEntries());
        ByteBuffer headBuffer = ByteBuffer.allocate(page.sizeOfHeader());

        // Add primitive fields
        headBuffer.putInt(page.getPageID()); // Serialize pageID as 4 bytes (int)
        headBuffer.putShort(page.size()); // Serialize numOfEntries as 2 bytes (short)
        headBuffer.putInt(page.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        headBuffer.flip();

        // Add entries
        for (Entry<K> entryObj : page.getAll()) {
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

        ByteBuffer combinedArray = ByteBuffer.allocate(page.sizeInBytes());
        // Get the remaining bytes from buffer1 and buffer2 into the combinedArray
        combinedArray.put(headBuffer.array());
        combinedArray.put(buffer.array());
        combinedArray.flip();
        // Return the underlying byte array
        return combinedArray.array();
    }

    public Page<K> bufferToPage(byte[] bufferData, Table table) throws IOException{
        if (bufferData == null || bufferData.length == 0) throw new IllegalArgumentException("Buffer data cannot be null or empty.");
        if (bufferData.length%4096 != 0) throw new IllegalArgumentException("Buffer data must be a modulo of a Blocks Size(4096 BYTES) you gave : "+bufferData.length);
        if (table == null || table.getSchema() == null) throw new IllegalArgumentException("Table or table schema cannot be null.");

        //Reading The page ID.
        int pageID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //Initializing New Empty Page.
        Page<K> newPage = new Page<>(pageID, table);

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
                        entry.add((byte[]) Arrays.copyOfRange(bufferData, startIndex, startIndex + byteSize));
                        startIndex += byteSize;
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected type: " + type);
                }
            }
            Entry<K> newEntry = new Entry<>(entry, table.getMaxIDSize());
            newEntry.setID(table.getIDindex());
            newPage.add(newEntry);
        }
        if(spaceInUse != newPage.getSpaceInUse()) throw new IOException("Mismatch between expected and actual space in use for Page.");
        if(numOfEntries != newPage.size()) throw new IOException("Mismatch between expected and actual numOfEntries and Page.size().");
        return newPage;
    }
    
}
