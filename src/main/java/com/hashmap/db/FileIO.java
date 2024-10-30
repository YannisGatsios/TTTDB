package com.hashmap.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class FileIO {

    public byte[] blockToBuffer(Block block) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(block.sizeOfEntries());
        ByteBuffer headBuffer = ByteBuffer.allocate(block.sizeOfHeader());

        byte[] indexBytes = (String.join(";",block.getIndexOfEntries())).getBytes(StandardCharsets.UTF_8);
        // Add primitive fields
        headBuffer.putInt(block.getBlockID()); // Serialize blockID as 4 bytes (int)
        headBuffer.putShort(block.getNumOfEtries()); // Serialize numOfEtries as 2 bytes (short)
        headBuffer.putInt(block.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        // Add indexOfEntries
        headBuffer.putInt(block.sizeOfHeader());//this is where the size of the header of the file gets saved
        headBuffer.put(indexBytes); // Then, store the actual string bytes

        headBuffer.flip();

        // Add entries
        for (ArrayList<Object> entry : block.getEntries()) {
            int ind = 0;
            for (Object element : entry) {
                if(ind == 0){
                    buffer.put((byte[]) element);
                }else if (element instanceof Integer) {
                    buffer.putInt((Integer) element); // Add integer elements as 4 bytes
                } else if (element instanceof byte[]) {
                    byte[] byteArray = (byte[]) element;
                    buffer.putShort((short) byteArray.length); // Store length of byte array
                    buffer.put((byte[]) element); // Add short elements as 2 bytes
                } else if (element instanceof String) {
                    byte[] strBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
                    buffer.putShort((short) strBytes.length); // Store length of string
                    buffer.put(strBytes); // Then the string bytes
                } else {
                    throw new IOException("Unsupported data type in entry.");
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
        int blockID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        Block newBlock = new Block(blockID, table.getMaxNumOfEntriesPerBlock());
        short numOfEtries = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 2)).getShort();
        bufferData = Arrays.copyOfRange(bufferData, 2, bufferData.length);

        int spaceInUse = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        int indexesSize = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt() - (3 * (Integer.BYTES) + Short.BYTES);
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        String indexString = new String(Arrays.copyOfRange(bufferData, 0, indexesSize), StandardCharsets.UTF_8);
        ArrayList<String> indexOfEntries = new ArrayList<String>(Arrays.asList(indexString.split(";")));
        bufferData = Arrays.copyOfRange(bufferData, indexesSize, bufferData.length);

        int startIndex = 0;
        for(int i = 0;i < numOfEtries;i++){
            ArrayList<Object> entry = new ArrayList<>();
            int endindex = Integer.parseInt(indexOfEntries.get(i).split(":")[1].trim());
            
            int ind = 0;
            for (String type : table.getColumnTypes()) {
                if(ind == 0){
                    int size = indexOfEntries.get(i).split(":")[0].trim().length();
                    entry.add(new String(Arrays.copyOfRange(bufferData, startIndex, startIndex + size), StandardCharsets.UTF_8));
                    startIndex += size;
                }else if(type.equals("String")){
                    int size = (int) ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex+2)).getShort();
                    startIndex += 2;
                    entry.add(new String(Arrays.copyOfRange(bufferData, startIndex, startIndex + size), StandardCharsets.UTF_8));
                    startIndex += size;
                }else if(type.equals("Integer")){
                    entry.add(ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex + 4)).getInt());
                    startIndex += 4;
                }else if(type.equals("Byte")){
                    int size = (int) ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, startIndex+2)).getShort();
                    startIndex += 2;
                    entry.add((byte[])Arrays.copyOfRange(bufferData, startIndex, startIndex + size));
                    startIndex += size;
                }
                ind++;
            }
            Entry newEntry = new Entry(entry);
            newBlock.AddEntry(newEntry);
            startIndex = endindex;
        }
        if(spaceInUse != newBlock.getSpaceInUse()){
            //TODO
            throw new IOException();
        }
        return newBlock;
    }
}
