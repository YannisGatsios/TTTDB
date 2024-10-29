package com.hashmap.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class FileIO {

    public byte[] blockToBuffer(Block block) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(block.sizeOfEntries());
        ByteBuffer headBuffer = ByteBuffer.allocate(block.sizeOfHeader());//The +4 for is for an extra integer that holds the size of the header of the file.

        // Add primitive fields
        headBuffer.putInt(block.getBlockID()); // Serialize blockID as 4 bytes (int)
        headBuffer.putShort(block.getNumOfEtries()); // Serialize numOfEtries as 2 bytes (short)
        headBuffer.putInt(block.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)

        // Add indexOfEntries
        byte[] indexBytes = (String.join(";",block.getIndexOfEntries())).getBytes(StandardCharsets.UTF_8);
        headBuffer.putInt(block.sizeOfHeader());//this is where the size of the header of the file gets saved
        headBuffer.put(indexBytes); // Then, store the actual string bytes
        headBuffer.flip();

        // Add entries
        for (ArrayList<Object> entry : block.getEntries()) {
            for (Object element : entry) {
                if (element instanceof Integer) {
                    buffer.putInt((Integer) element); // Add integer elements as 4 bytes
                } else if (element instanceof byte[]) {
                    buffer.put((byte[]) element); // Add short elements as 2 bytes
                } else if (element instanceof String) {
                    byte[] strBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
                    buffer.putShort((short) strBytes.length); // Store length of string
                    buffer.put(strBytes); // Then the string bytes
                } else {
                    throw new IOException("Unsupported data type in entry.");
                }
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

    public Block bufferToBlock(byte[] bufferData, Table table){
        int blockID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        Block newBlock = new Block(blockID, table.getMaxNumOfEntriesPerBlock());
        newBlock.setNumOfCumlumns(table.getNumOfColumns());
        newBlock.setNumOfEntries(ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 2)).getShort());
        bufferData = Arrays.copyOfRange(bufferData, 2, bufferData.length);

        newBlock.setSpaceInUse(ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt());
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        int size = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);
        String indexString = new String(Arrays.copyOfRange(bufferData, 0, size-14), StandardCharsets.UTF_8);
        newBlock.setIndexOfEntries(new ArrayList<String>(Arrays.asList(indexString.split(";"))));

        int startIndex = 0;
        for(int i = 0;i < newBlock.getNumOfEtries();i++){
            String[] curEntry = (newBlock.getIndexOfEntries().get(0).split(":")[1]).split(",");
            int endindex = Integer.parseInt(curEntry[table.getNumOfColumns()-1]);

            ArrayList<Object> entry = new ArrayList<>();
            byte[] entryData = Arrays.copyOfRange(bufferData, 0, endindex-startIndex);
            int ind = 0;
            for (String type : table.getColumnTypes()) {
                if(type.equals("String")){
                    entry.add(new String(Arrays.copyOfRange(entryData, startIndex, Integer.parseInt(curEntry[ind])), StandardCharsets.UTF_8));
                }else if(type.equals("Integer")){
                    entry.add(ByteBuffer.wrap(Arrays.copyOfRange(entryData, startIndex, Integer.parseInt(curEntry[ind]))).getInt());
                }else if(type.equals("Byte")){
                    entry.add((byte[])Arrays.copyOfRange(bufferData, startIndex, Integer.parseInt(curEntry[ind])));
                }
                startIndex = Integer.parseInt(curEntry[ind]);
                ind++;
                System.out.print("\n"+entry+"\n");
            }
        }

        return newBlock;
    }
}
