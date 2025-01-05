package com.database.db;

import java.util.Random;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;

import com.database.db.bPlusTree.BPlusTree;
import com.database.db.bPlusTree.Tree;
import com.database.db.page.Page;
import com.database.db.parser.DBMSprocesses;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class App {
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = SECURE_RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException{
        FileIO fileIO = new FileIO();
        //Table INIT.
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        schema.printSchema();
        Table table = new Table(databaseName, tableName, schema);
        //Tree INIT.
        BPlusTree<String,Integer> tree = new Tree<>(table.getPageMaxNumOfEntries());
        tree = tree.bufferToTree(fileIO.readTree(table.getIndexPath()), table);
        DBMSprocesses<String> DBMS = new DBMSprocesses<>();

        Random random = new Random(); 
        String[] keysList = new String[400];
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(table.getMaxIDSize()-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!tree.isKey(userName)){
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
                Entry<String> entry = new Entry<>(entryData, table.getMaxIDSize());
                entry.setID(table.getIDindex());
                tree = DBMS.insertionProcess(table, entry, tree);
                ind++;
            }
        }
        byte[] treeBuffer = tree.treeToBuffer(tree, table.getMaxIDSize());
        fileIO.writeTree(table.getIndexPath(), treeBuffer);
        tree.printTree();
        
        BPlusTree<String,Integer> tree2 = new Tree<>(table.getPageMaxNumOfEntries());
        byte[] newTree = fileIO.readTree(table.getIndexPath());
        System.out.println(table.getIDtype()+new String().getClass().getName());
        tree2 = tree2.bufferToTree(newTree, table);
        //tree2.printTree();
        /* 
        int trues = 0;
        int falser = 0;
        for (String key : keysList) {
            if(tree2.search(key)){
                trues++;
            }else{
                falser++;
            }
            System.out.println("Key is "+key+" : "+tree2.findPair(key).getValue());
        }
        // System.out.println("True Are : "+trues+"\nFalse Are : "+falser);*/

        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(399);
            if(tree2.isKey(keysList[randInd])){
                System.out.println(ind+" : Random Index : "+keysList[randInd]+" : "+tree2.search(keysList[randInd]));
                tree2 = DBMS.deletionProcess(table, tree2, keysList[randInd]);
                ind++;
            }
        }
        fileIO.writeTree(table.getIndexPath(), tree2.treeToBuffer(tree2, table.getMaxIDSize()));

        /*int i = 0;
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
        tree.printTree();

        
        //writing tree1
        tree.lastPageID = 10;
        String indexPath = "storage/index/"+table.getDatabaseName()+"."+table.getTableName()+".index";
        byte[] bufferTree = tree.treeToBuffer(tree, table.getMaxIDSize(), table.getMaxEntriesPerPage());
        tree.writeTree(indexPath, bufferTree);

        //creating and reading tree2
        BPlusTree tree2 = new BPlusTree(3);
        bufferTree = tree2.readTree(indexPath);
        tree2 = tree2.bufferToTree(bufferTree, 5);*/
        //tree.printTree();*/
        
        
        //entry 1
        ArrayList<Object> entryData1 = new ArrayList<>();
        entryData1.add("johnttt1");
        entryData1.add(101);
        entryData1.add("hello");
        byte[] buffer = new byte[10];
        buffer[9] = (byte) 0xff;
        entryData1.add(buffer);
        Entry<String> entry1 = new Entry<>(entryData1, table.getMaxIDSize());
        entry1.setID(table.getIDindex());

        //entry 2
        ArrayList<Object> entryData2 = new ArrayList<>();
        entryData2.add("johnttt22");
        entryData2.add(102);
        entryData2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entryData2.add(buffer2);
        Entry<String> entry2 = new Entry<>(entryData2, table.getMaxIDSize());
        entry2.setID(table.getIDindex());

        //entry 3
        ArrayList<Object> entryData3 = new ArrayList<>();
        entryData3.add("johnttt333");
        entryData3.add(103);
        entryData3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entryData3.add(buffer2);
        Entry<String> entry3 = new Entry<>(entryData3, table.getMaxIDSize());
        entry3.setID(table.getIDindex());
        
        //Page
        Page<String> page = new Page<>(0, table);

        try {
            page.add(entry1);
            page.add(entry2);
            page.add(entry3);

            page.remove(0);
            //page.removeEntry(1);
            //page.removeEntry(2);
            System.out.println(page.pageStats());
            
            //Writing page to memory 
            byte[] data = page.pageToBuffer(page);
            String tablePath = "storage/" + table.getDatabaseName() + "." + table.getTableName() + ".table";
            fileIO.writePage(tablePath, data, page.sizeInBytes());
            
            //Reading page from memory
            Page<String> newPage = new Page<>(0, table);
            byte[] bufferPage = fileIO.readPage(tablePath, page.getPagePos(), page.sizeInBytes());
            newPage = newPage.bufferToPage(bufferPage, table);
            System.out.println("NEW PAGE:"+newPage.pageStats());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
