package com.hashmap.db;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) {
        String tableConfig = "dataBase;users;username_id:10:String;num:6:Integer;message:5:String;data:10:Byte";
        Table table = new Table(tableConfig.split(";"));
        table.setMaxNumOfEntriesPerBlock((short)10);
        table.setSizeOfIndexPerElement((byte)3);

        //entry 1
        ArrayList<Object> entrie = new ArrayList<>();
        entrie.add("johntttGR");
        entrie.add(100);
        entrie.add("hello");
        byte[] buffer = new byte[10];
        buffer[9] = (byte) 0xff;
        entrie.add(buffer);

        Entry entry1 = new Entry(entrie);

        //entry 2
        ArrayList<Object> entrie2 = new ArrayList<>();
        entrie2.add("johnttt");
        entrie2.add(101);
        entrie2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entrie2.add(buffer2);

        Entry entry2 = new Entry(entrie2);
        

        //block
        Block block = new Block(1, (short)table.getMaxNumOfEntriesPerBlock());
        block.setNumOfCumlumns(table.getNumOfColumns());
        block.setSizeOfIndexPerElement(table.getSizeOfIndexPerElement());
        block.setMaxSizeOfEntry(table.getMaxSizeOfEntry());
        try {
            block.AddEntry(entry1);
            block.AddEntry(entry2);
            //block.removeEntry(entry1.getID());
            //block.removeEntry(entry2.getID());
            byte[] data = block.blockToBuffer(block);
            for (byte b : data) {
                //System.out.printf("%02X ", b); // Print in hexadecimal format
            }
            Block newBlock = block.bufferToBlock(data, table);
            //System.out.println(newBlock.blockStats());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //System.out.print(block.getEntries());
    }
}
