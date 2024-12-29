package com.database.db;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.io.IOException;
import java.util.ArrayList;

import com.database.db.bPlusTree.BPlusTree;
import com.database.db.bPlusTree.TreeUtils.Pair;
import com.database.db.page.Page;

public class App {

    public static void main(String[] args) {

        Pair<Byte[], Integer> pair = new Pair<>(new Byte[]{20}, 0);
        System.out.println(pair.toString());
        
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
        //tree.printTree();

        // Search for a key
        byte[] searchKey = {15};
        System.out.println("\nSearching for key " + Arrays.toString(searchKey) + ": " + (tree.search(searchKey) ? "Found" : "Not Found"));

        // Perform a range query
        byte[] lower = {10}, upper = {30};
        List<byte[]> rangeResult = tree.rangeQuery(lower, upper);
        System.out.println("\nRange query [" + Arrays.toString(lower) + ", " + Arrays.toString(upper) + "]: ");
        for (byte[] bs : rangeResult) {
            System.out.print(Arrays.toString(bs));
        }

        // Remove a key
        byte[] removeKey = {15};
        tree.remove(removeKey);
        System.out.println("\nB+ Tree after removing " + Arrays.toString(removeKey) + ":");
        ///tree.printTree();
        
        /* 
        
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:fasle:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        //Schema schema = new Schema("num:4:Integer;username:10:String;message:5:String;data:10:Byte".split(";"));
        schema.printSchema();
        Table table = new Table(databaseName, tableName, schema);

        //entry 1
        ArrayList<Object> entrie = new ArrayList<>();
        entrie.add("johnttt1");
        entrie.add(101);
        entrie.add("hello");
        byte[] buffer = new byte[10];
        buffer[9] = (byte) 0xff;
        entrie.add(buffer);
        Entry entry1 = new Entry(entrie, table.getMaxIDSize());
        entry1.setID(table.getIDindex());

        //entry 2
        ArrayList<Object> entrie2 = new ArrayList<>();
        entrie2.add("johnttt22");
        entrie2.add(102);
        entrie2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entrie2.add(buffer2);
        Entry entry2 = new Entry(entrie2, table.getMaxIDSize());
        entry2.setID(table.getIDindex());

        //entry 3
        ArrayList<Object> entrie3 = new ArrayList<>();
        entrie3.add("johnttt333");
        entrie3.add(103);
        entrie3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entrie3.add(buffer2);
        Entry entry3 = new Entry(entrie3, table.getMaxIDSize());
        entry3.setID(table.getIDindex());
        
        //Page
        Page page = new Page(0, (short) table.getMaxEntriesPerPage());
        page.setMaxSizeOfEntry(table.getSizeOfEntry());

        try {
            page.addEntry(entry1);
            page.addEntry(entry2);
            page.addEntry(entry3);

            page.removeEntry(0);
            //page.removeEntry(1);
            //page.removeEntry(2);
            System.out.println(page.pageStats());
            
            //Writing page to memory 
            byte[] data = page.pageToBuffer(page);
            String path = "storage/" + table.getDatabaseName() + "." + table.getTableName() + ".table";
            page.writePage(path, data, page.getPageID()*page.sizeOfPage());
            
            //Reading page from memory
            Page newPage = new Page(0, (short)table.getMaxEntriesPerPage());
            byte[] bufferPage = newPage.readPage(path, page.getPageID() * page.sizeOfPage(), page.sizeOfPage());
            newPage = newPage.bufferToPage(bufferPage, table);
            System.out.println("NEW PAGE:"+newPage.pageStats());
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
