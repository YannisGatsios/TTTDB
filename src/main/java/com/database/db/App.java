package com.database.db;

//import java.util.Arrays;
//import java.util.List;
import java.util.Random;

import javax.crypto.spec.PBEKeySpec;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;

import com.database.db.bPlusTree.BPlusTree;
//import com.database.db.bPlusTree.TreeUtils.Pair;
//import com.database.db.page.Page;
import com.database.db.parser.DBMSprocesses;

public class App {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    public static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = SECURE_RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException{
        //Table INIT.
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:fasle:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        schema.printSchema();
        Table table = new Table(databaseName, tableName, schema);
        //Tree INIT.
        BPlusTree tree = new BPlusTree(table.getMaxEntriesPerPage());
        DBMSprocesses DBMS = new DBMSprocesses();

        Random random = new Random();
        String[] keysList = new String[400];
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(table.getMaxIDSize()-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!tree.search(userName)){
                keysList[ind] = userName;
                ArrayList<Object> entryData = new ArrayList<>();
                entryData.add(userName);

                int intNum = random.nextInt();
                entryData.add(intNum);

                int sizeOfString = random.nextInt(table.getSchema().getColumnSizes()[2]);
                String randStr = generateRandomString(sizeOfString);
                entryData.add(randStr);

                int sizeOfData = random.nextInt(table.getSchema().getColumnSizes()[3]);
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                entryData.add(data);
                Entry entry = new Entry(entryData, table.getMaxIDSize());
                entry.setID(table.getIDindex());
                tree = DBMS.insertionProcess(table, entry, tree);
                ind++;
            }
        }
        byte[] treeBuffer = tree.treeToBuffer(tree, table.getMaxIDSize(), table.getMaxEntriesPerPage());
        tree.writeTree(table.getIndexPath(), treeBuffer);
        tree.printTree();
        int trues = 0;
        int falses = 0;
        for (String key : keysList) {
            if(tree.search(key)){
                trues++;
            }else{
                falses++;
            }
        }
        System.out.println("True Are : "+trues+"\nFalse Are : "+falses);
        
        BPlusTree tree2 = new BPlusTree(table.getMaxEntriesPerPage());
        byte[] newTree = tree2.readTree(table.getIndexPath());
        tree2 = tree2.bufferToTree(newTree, table.getMaxEntriesPerPage(), table.getIDtype());
        //tree2.printTree();
        for (String key : keysList) {
            System.out.println("The Key : "+key+"\nIs Stored At Page : "+tree2.findPair(key).getValue());
        }
        ind = 0;
        /* 
        while(i < 400){
            int num = random.nextInt(127);
            int num1 = random.nextInt(127);
            byte[] data = {(byte) num,(byte) num1};
            if(!tree.search(data)){
                tree.insert(new Pair<byte[],Integer>(data, i));
                i++;
            }
        }

        // Insert elements
        tree.insert(new Pair<byte[],Integer>(new byte[] {10}, 1));
        tree.insert(new Pair<byte[],Integer>(new byte[] {20}, 2));
        tree.insert(new Pair<byte[],Integer>(new byte[] {5}, 3));
        tree.insert(new Pair<byte[],Integer>(new byte[] {15}, 4));
        tree.insert(new Pair<byte[],Integer>(new byte[] {25}, 5));
        tree.insert(new Pair<byte[],Integer>(new byte[] {30}, 6));
        tree.insert(new Pair<byte[],Integer>(new byte[] {11}, 7));
        tree.insert(new Pair<byte[],Integer>(new byte[] {22}, 8));
        tree.insert(new Pair<byte[],Integer>(new byte[] {6}, 9));
        tree.insert(new Pair<byte[],Integer>(new byte[] {16}, 10));
        tree.insert(new Pair<byte[],Integer>(new byte[] {26}, 11));
        tree.insert(new Pair<byte[],Integer>(new byte[] {31}, 12));

        System.out.println("B+ Tree after insertions:");
        tree.printTree();

        // Search for a key
        byte[] searchKey = {15};
        System.out.println("\nSearching for key " + Arrays.toString(searchKey) + ": " + (tree.search(searchKey) ? "Found" : "Not Found"));

        // Perform a range query
        byte[] lower = {10}, upper = {30};
        List<Pair<?,Integer>> rangeResult = tree.rangeQuery(lower, upper);
        System.out.println("\nRange query [" + Arrays.toString(lower) + ", " + Arrays.toString(upper) + "]: ");
        for (Pair<?,Integer> bs : rangeResult) {
            System.out.print(" { "+Arrays.toString(tree.keyToByteArray(bs.getKey()))+" : " + bs.getValue() + " } ");
        }

        // Remove a key
        byte[] removeKey = {15};
        tree.remove(removeKey);
        System.out.println("\nB+ Tree after removing " + Arrays.toString(removeKey) + ":");

        //writing tree1
        tree.lastPageID = 10;
        String indexPath = "storage/index/"+table.getDatabaseName()+"."+table.getTableName()+".index";
        byte[] bufferTree = tree.treeToBuffer(tree, table.getMaxIDSize(), table.getMaxEntriesPerPage());
        tree.writeTree(indexPath, bufferTree);

        //creating and readin tree2
        BPlusTree tree2 = new BPlusTree(3);
        bufferTree = tree2.readTree(indexPath);
        tree2 = tree2.bufferToTree(bufferTree, 5);*/
        //tree.printTree();
        
        /* 
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
            String tablePath = "storage/" + table.getDatabaseName() + "." + table.getTableName() + ".table";
            page.writePage(tablePath, data, page.getPageID()*page.sizeOfPage());
            
            //Reading page from memory
            Page newPage = new Page(0, (short)table.getMaxEntriesPerPage());
            byte[] bufferPage = newPage.readPage(tablePath, page.getPageID() * page.sizeOfPage(), page.sizeOfPage());
            newPage = newPage.bufferToPage(bufferPage, table);
            System.out.println("NEW PAGE:"+newPage.pageStats());
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
