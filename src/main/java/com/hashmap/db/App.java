package com.hashmap.db;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) {
        String tableConfig = "dataBase;users;username:10:String;num:4:Integer;message:5:String;data:10:Byte";
        Table table = new Table(tableConfig.split(";"));
        table.setMaxNumOfEntriesPerBlock((short)3);

        //entry 1
        ArrayList<Object> entrie = new ArrayList<>();
        entrie.add("johnttt1");
        entrie.add(101);
        entrie.add("hello");
        byte[] buffer = new byte[10];
        buffer[9] = (byte) 0xff;
        entrie.add(buffer);

        Entry entry1 = new Entry(entrie);

        //entry 2
        ArrayList<Object> entrie2 = new ArrayList<>();
        entrie2.add("johnttt22");
        entrie2.add(102);
        entrie2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entrie2.add(buffer2);

        Entry entry2 = new Entry(entrie2);

        ArrayList<Object> entrie3 = new ArrayList<>();
        entrie3.add("johnttt333");
        entrie3.add(103);
        entrie3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entrie3.add(buffer2);

        Entry entry3 = new Entry(entrie3);
        

        //block
        Block block = new Block(1, (short)table.getMaxNumOfEntriesPerBlock());
        block.setMaxSizeOfEntry(table.getMaxSizeOfEntry());
        block.setMaxSizeOfID(table.getColumnSizes()[0]);
        try {
            block.AddEntry(entry1);
            block.AddEntry(entry2);
            block.AddEntry(entry3);
            
            //block.removeEntry(entry1.getID());
            //block.removeEntry(entry2.getID());
            
            byte[] data = block.blockToBuffer(block);
            for (byte b : data) {
                System.out.printf("%02X ", b); // Print in hexadecimal format
            }
            System.out.println(block.blockStats());
            Block newBlock = block.bufferToBlock(data, table);
            System.out.println(newBlock.blockStats());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
