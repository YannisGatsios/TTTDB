package com.database.db;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.io.IOException;
import java.util.ArrayList;

import com.database.db.bPlusTree.BPlusTree;
import com.database.db.block.Block;

public class App {

    public static void main(String[] args) {
        
        BPlusTree tree = new BPlusTree(5);
        Random random = new Random();
        int i = 0;
        while(i < 400){
            int num = random.nextInt(127);
            int num1 = random.nextInt(127);
            byte[] data = {(byte) num,(byte) num1};
            if(!tree.search(data)){
                tree.insert(data);
                i++;
            }
        }
        

        // Insert elements
        tree.insert(new byte[] {10});
        tree.insert(new byte[] {20});
        tree.insert(new byte[] {5});
        tree.insert(new byte[] {15});
        tree.insert(new byte[] {25});
        tree.insert(new byte[] {30});
        tree.insert(new byte[] {11});
        tree.insert(new byte[] {22});
        tree.insert(new byte[] {6});
        tree.insert(new byte[] {16});
        tree.insert(new byte[] {26});
        tree.insert(new byte[] {31});

        System.out.println("B+ Tree after insertions:");
        tree.printTree();

        // Search for a key
        byte[] searchKey = {100};
        System.out.println("\nSearching for key " + Arrays.toString(searchKey) + ": " + (tree.search(searchKey) ? "Found" : "Not Found"));

        // Perform a range query
        byte[] lower = {10}, upper = {30};
        List<byte[]> rangeResult = tree.rangeQuery(lower, upper);
        System.out.println("\nRange query [" + Arrays.toString(lower) + ", " + Arrays.toString(upper) + "]: ");
        for (byte[] bs : rangeResult) {
            System.out.print(Arrays.toString(bs));
        }

        // Remove a key
        byte[] removeKey = {20};
        tree.remove(removeKey);
        System.out.println("\nB+ Tree after removing " + Arrays.toString(removeKey) + ":");
        
        

        /* 
        String databaseName = "database";
        String tableName = "users";

        Schema schema = new Schema("username:10:String;num:4:Integer;message:5:String;data:10:Byte".split(";"));
        //Schema schema = new Schema("num:4:Integer;username:10:String;message:5:String;data:10:Byte".split(";"));

        Table table = new Table(databaseName, tableName, schema);
        table.setMaxNumOfEntriesPerBlock((short)3);

        System.out.println(table.getSchema().getColumnSizes()+"===================");

        //entry 1
        ArrayList<Object> entrie = new ArrayList<>();
        entrie.add("johnttt1");
        entrie.add(101);
        entrie.add("hello");
        byte[] buffer = new byte[10];
        buffer[9] = (byte) 0xff;
        entrie.add(buffer);
        Entry entry1 = new Entry(entrie, table.getMaxIDSize());

        //entry 2
        ArrayList<Object> entrie2 = new ArrayList<>();
        entrie2.add("johnttt22");
        entrie2.add(102);
        entrie2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entrie2.add(buffer2);
        Entry entry2 = new Entry(entrie2, table.getMaxIDSize());

        ArrayList<Object> entrie3 = new ArrayList<>();
        entrie3.add("johnttt333");
        entrie3.add(103);
        entrie3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entrie3.add(buffer2);
        Entry entry3 = new Entry(entrie3, table.getMaxIDSize());
        

        //block
        Block block = new Block(0, (short)table.getMaxNumOfEntriesPerBlock());
        block.setMaxSizeOfEntry(table.getMaxSizeOfEntry());
        block.setMaxSizeOfID(table.getMaxIDSize());
        try {
            block.addEntry(entry1);
            block.addEntry(entry2);
            block.addEntry(entry3);

            //block.removeEntry(entry1.getID());
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
        }*/
    }
}
