package com.hashmap.db;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class FileIO {

    private byte[] concatenate(byte[] array1, byte[] array2) {
        // Create a ByteBuffer with the combined size of both arrays
        ByteBuffer buffer = ByteBuffer.allocate(array1.length + array2.length);
        
        // Put both arrays into the buffer
        buffer.put(array1);
        buffer.put(array2);
        
        return buffer.array(); // Return the underlying byte array
    }

    public byte[] blockToBuffer(Block block) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(block.sizeOfEntries());
        ByteBuffer headBuffer = ByteBuffer.allocate(block.sizeOfHeader());

        // Add primitive fields
        headBuffer.putInt(block.getBlockID()); // Serialize blockID as 4 bytes (int)
        headBuffer.putShort(block.getNumOfEtries()); // Serialize numOfEtries as 2 bytes (short)
        headBuffer.putInt(block.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        // Add Size Of Header
        headBuffer.putInt(block.sizeOfHeader());//this is where the size of the header of the file gets saved
        //Adding the Index Of Eatch Entry
        HashMap<byte[], Short> indexOfEntries = block.getIndexOfEntries();
        for (ArrayList<Object> entry : block.getEntries()) {
            headBuffer.put(this.concatenate((byte[])entry.get(0),ByteBuffer.allocate(2).putShort(indexOfEntries.get(entry.get(0))).array()));//indexes are saved in order
        }
        headBuffer.flip();

        // Add entries
        for (ArrayList<Object> entry : block.getEntries()) {
            int ind = 0;
            for (Object element : entry) {
                if(ind == 0){
                    buffer.put((byte[]) element);
                }else {
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
                ind++;
            }
        }
        buffer.flip();

        ByteBuffer combinedArray = ByteBuffer.allocate(block.getSizeOfBlock());

        // Get the remaining bytes from buffer1 and buffer2 into the combinedArray
        combinedArray.put(headBuffer.array());
        combinedArray.put(buffer.array());
        combinedArray.flip();

        // Return the underlying byte array
        return combinedArray.array();
    }

    public Block bufferToBlock(byte[] bufferData, Table table) throws IOException{
        //Reading The Blocks ID.
        int blockID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //Initializing New Empty Block.
        Block newBlock = new Block(blockID, table.getMaxNumOfEntriesPerBlock());
        newBlock.setMaxSizeOfEntry(table.getMaxSizeOfEntry());
        newBlock.setMaxSizeOfID(table.getColumnSizes()[0]);

        //Reading The Number Of Entries.
        short numOfEtries = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 2)).getShort();
        bufferData = Arrays.copyOfRange(bufferData, 2, bufferData.length);

        //Reading The Space In Use Of The Block.
        int spaceInUse = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //==Reading Indexes==
        //Reading Size Of Indexes Part.
        int indexesSize = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt() - 14;
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        //Reading The Sctuale Indexes.
        ArrayList<ArrayList<Object>> indexOfEntries = new ArrayList<>(numOfEtries);//Oredered indexes are saved here
        int startIndex = 0;
        for(int i = 0; i < numOfEtries; i++){
            ArrayList<Object> entryInd = new ArrayList<>(2);
            //Getting The Entry ID.
            byte[] entryID = new byte[newBlock.getMazSizeOfID()];
            entryID = Arrays.copyOfRange(bufferData, startIndex, startIndex + entryID.length);
            startIndex += entryID.length;

            //Getting Index Corresponding To That ID
            short index = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex+2)).getShort();// because indexes are saved in order when they are read they are in order.
            startIndex += 2;

            entryInd.add(entryID);
            entryInd.add(index);
            indexOfEntries.add(entryInd);
        }
        bufferData = Arrays.copyOfRange(bufferData, indexesSize, bufferData.length);

        //==Reading The Actual Entries based On The IndexList==
        startIndex = 0;
        for(int i = 0; i < numOfEtries; i++){
            ArrayList<Object> entry = new ArrayList<>();
            short endindex = (short) indexOfEntries.get(i).get(1);
            
            int ind = 0;
            for (String type : table.getColumnTypes()) {
                if(ind == 0){
                    int size = ((byte[])indexOfEntries.get(i).get(0)).length;
                    entry.add(new String(Arrays.copyOfRange(bufferData, startIndex, startIndex + size), StandardCharsets.UTF_8));
                    startIndex += size;
                }else{
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
                ind++;
            }
            Entry newEntry = new Entry(entry, table.getMazSizeOfID());
            newBlock.addEntry(newEntry);
            startIndex = endindex;
        }
        if(spaceInUse != newBlock.getSpaceInUse()){
            //TODO
            throw new IOException();
        }
        return newBlock;
    }
}
