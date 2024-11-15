package com.database.db;

import java.io.IOException;
import java.util.ArrayList;

import com.database.db.block.Block;

/**
 * Hello world!
 *
 */
public class App {

    public static void main(String[] args) {
        String tableConfig = "database;users;username:10:String;num:4:Integer;message:5:String;data:10:Byte";//String ID
        //String tableConfig = "database;users;num:4:Integer;username:10:String;message:5:String;data:10:Byte";//Integer ID
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
        Entry entry1 = new Entry(entrie, table.getMazSizeOfID());

        //entry 2
        ArrayList<Object> entrie2 = new ArrayList<>();
        entrie2.add("johnttt22");
        entrie2.add(102);
        entrie2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entrie2.add(buffer2);
        Entry entry2 = new Entry(entrie2, table.getMazSizeOfID());

        ArrayList<Object> entrie3 = new ArrayList<>();
        entrie3.add("johnttt333");
        entrie3.add(103);
        entrie3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entrie3.add(buffer2);
        Entry entry3 = new Entry(entrie3, table.getMazSizeOfID());
        

        //block
        Block block = new Block(0, (short)table.getMaxNumOfEntriesPerBlock());
        block.setMaxSizeOfEntry(table.getMaxSizeOfEntry());
        block.setMaxSizeOfID(table.getColumnSizes()[0]);
        try {
            block.addEntry(entry1);
            block.addEntry(entry2);
            block.addEntry(entry3);

            block.removeEntry(entry1.getID());
            //block.removeEntry(entry3.getID());
            //block.removeEntry(entry2.getID());
            System.out.println(block.blockStats());
            
            byte[] data = block.blockToBuffer(block);
            String path = "storage/" + table.getDatabase() + "." + table.getTableName() + ".tb";
            block.writeBlock(path, data, block.getBlockID()*block.getSizeOfBlock());
            
            Block newBlock = new Block(0, (short)table.getMaxNumOfEntriesPerBlock());
            byte[] bufferBlock = newBlock.readBlock(path, block.getBlockID() * block.getSizeOfBlock(), block.getSizeOfBlock());
            newBlock = newBlock.bufferToBlock(bufferBlock, table);
            System.out.println("NEW_BLOCK:\n"+newBlock.blockStats());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
