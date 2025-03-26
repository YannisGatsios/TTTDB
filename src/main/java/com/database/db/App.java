package com.database.db;

import java.util.Random;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;

import com.database.db.index.PrimaryKey;
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
    @SuppressWarnings("unchecked")
    public static <K extends Comparable<K>> void main(String[] args) throws IOException{
        FileIO fileIO = new FileIO();
        //Table INIT.
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        schema.printSchema();
        Table table = new Table(databaseName, tableName, schema);
        //Tree INIT.
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        DBMSprocesses DBMS = new DBMSprocesses();

        Random random = new Random(); 
        String[] keysList = new String[400];
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(table.getPrimaryKeyMaxSize()-1)+1;
            String userName = generateRandomString(sizeOfID);
            if(!tree.isKey((K)userName)){
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
                Entry<String> entry = new Entry<>(entryData, table.getPrimaryKeyMaxSize());
                entry.setID(table.getPrimaryKeyColumnIndex());
                DBMS.insertionProcess(table, entry);
                ind++;
            }
        }
        byte[] treeBuffer = tree.treeToBuffer(table.getPrimaryKeyMaxSize());
        fileIO.writeTree(table.getIndexPath(), treeBuffer);
        System.out.println(tree.toString());
        
        PrimaryKey<K> tree2 = new PrimaryKey<>(table.getPageMaxNumOfEntries());
        byte[] newTree = fileIO.readTree(table.getIndexPath());
        System.out.println(table.getPrimaryKeyType()+new String().getClass().getName());
        tree2 = tree2.bufferToTree(newTree, table);
        //tree2.printTree();
        int trues = 0;
        int falser = 0;
        for (String key : keysList) {
            if(tree2.isKey((K)key)){
                trues++;
            }else{
                falser++;
            }
            //System.out.println("Key is "+key+" : "+tree2.search((K)key));
        }
        System.out.println("True Are : "+trues+"\nFalse Are : "+falser);
        table.setPrimaryKeyIndex(tree2);
        ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(399);
            if(tree2.isKey((K)keysList[randInd])){
                System.out.println(ind+" : Random Index : "+keysList[randInd]+" : "+tree2.search((K)keysList[randInd]));
                DBMS.deletionProcess(table, keysList[randInd]);
                ind++;
            }
        }
        fileIO.writeTree(table.getIndexPath(), tree2.treeToBuffer(table.getPrimaryKeyMaxSize()));

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
    }
}
