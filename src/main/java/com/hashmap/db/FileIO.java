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
                    //buffer.putShort((short) strBytes.length); // Store length of string
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

    public Block bufferToBlock(byte[] bufferData, Table table) throws IOException{
        int blockID = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        Block newBlock = new Block(blockID, table.getMaxNumOfEntriesPerBlock());
        newBlock.setNumOfCumlumns(table.getNumOfColumns());
        short numOfEtries = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 2)).getShort();
        bufferData = Arrays.copyOfRange(bufferData, 2, bufferData.length);

        int spaceInUse = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);

        int size = ByteBuffer.wrap(Arrays.copyOfRange(bufferData, 0, 4)).getInt();
        bufferData = Arrays.copyOfRange(bufferData, 4, bufferData.length);
        String indexString = new String(Arrays.copyOfRange(bufferData, 0, size-14), StandardCharsets.UTF_8);
        ArrayList<String> indexOfEntries = new ArrayList<String>(Arrays.asList(indexString.split(";")));
        bufferData = Arrays.copyOfRange(bufferData, size-14, bufferData.length);

        int startIndex = 0;
        //byte[] entryData = Arrays.copyOfRange(bufferData, 0, endindex-startIndex);
        for(int i = 0;i < numOfEtries;i++){
            ArrayList<Object> entry = new ArrayList<>();

            String[] curEntry = (indexOfEntries.get(i).split(":")[1]).split(",");
            int endindex = Integer.parseInt(curEntry[(int) table.getNumOfColumns()-1].trim());
            
            int ind = 0;
            for (String type : table.getColumnTypes()) {
                int index = Integer.parseInt(curEntry[ind].trim());
                System.out.println(startIndex+"|==|"+index);
                if(type.equals("String")){
                    entry.add(new String(Arrays.copyOfRange(bufferData, startIndex, index), StandardCharsets.UTF_8));
                }else if(type.equals("Integer")){
                    entry.add(ByteBuffer.wrap(Arrays.copyOfRange(bufferData, startIndex, index)).getInt());
                }else if(type.equals("Byte")){
                    entry.add((byte[])Arrays.copyOfRange(bufferData, startIndex, index));
                }
                System.out.println(startIndex);
                startIndex = Integer.parseInt(curEntry[ind].trim());
                ind++;
            }
            System.out.println(entry);
            Entry newEntry = new Entry(entry);
            newBlock.AddEntry(newEntry);
            startIndex = endindex;
        }
        return newBlock;
    }
}
