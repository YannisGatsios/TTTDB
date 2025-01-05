package com.database.db.parser;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import com.database.db.Schema;
import com.database.db.Table;
import com.database.db.Entry;
import com.database.db.bPlusTree.BPlusTree;
import com.database.db.bPlusTree.Tree;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBMSprocessesTest {
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

    private Table table;
    private BPlusTree<String,Integer> tree;
    private DBMSprocesses<String> DBMS;
    private Random random;
    private static String[] keysList;
    private static String databaseName;
    private static String tableName;

    @BeforeEach
    void setup(){
        databaseName = "test_database";
        tableName = "test_table";
        Schema schema = new Schema("username:15:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        
        DBMS = new DBMSprocesses<>();
        DBMS.createTable(databaseName, tableName, schema); //Creating table

        table = new Table(databaseName, tableName, schema);//Table INIT
        tree = new Tree<>(table.getMaxEntriesPerPage());//Tree INIT
        random = new Random();
        keysList = new String[400];
    }

    @Test
    @Order(1)
    void testOneInsert(){
        ArrayList<Object> entryData = new ArrayList<>();
        entryData.add("firstEntry");
        entryData.add(100);
        entryData.add("HELLO");
        byte[] data = new byte[10];
        entryData.add(data);
        Entry<String> entry = new Entry<>(entryData, table.getMaxIDSize());
        entry.setID(table.getIDindex());
        try {
            tree = DBMS.insertionProcess(table, entry, tree);
            tree.writeTree(table.getIndexPath(), tree.treeToBuffer(tree, table.getMaxIDSize()));
        } catch (IllegalArgumentException e) {
            fail(e);
        } catch (IOException e) {
            fail(e);
        }
    }
    @Test
    @Order(2)
    void testDeleteLastEntry(){
        tree = tree.bufferToTree(tree.readTree(table.getIndexPath()), table);
        try {
            tree = DBMS.deletionProcess(table, tree, "firstEntry");
            tree.writeTree(table.getIndexPath(), tree.treeToBuffer(tree, table.getMaxIDSize()));
        } catch (IllegalArgumentException | IOException e) {
            fail(e);
        }
    }

    @Test
    @Order(3)
    void testRandomEntryInsertion() {
        tree = tree.bufferToTree(tree.readTree(table.getIndexPath()), table);
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
                try {
                    tree = DBMS.insertionProcess(table, entry, tree);
                } catch (IOException e) {
                    fail(e);
                }
                ind++;
            }
        }
        tree.writeTree(table.getIndexPath(),
                tree.treeToBuffer(tree, table.getMaxIDSize()));
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("DBMSprocessesTest.txt"))) {
            for (String key : keysList) {
                writer.write(key + "\n");
            }
        } catch (IOException e) {
            fail(e);
        }
    }

    //You must run testRandomEntryInsertion first.
    @Test
    @Order(4)
    void testRandomDeletion() {
        //Reading file with previously inserted key To check in reading from index file is working properly.
        try (BufferedReader reader = new BufferedReader(new FileReader("DBMSprocessesTest.txt"))) {
            List<String> keys = reader.lines().collect(Collectors.toList());
            keysList = keys.toArray(new String[0]);
        } catch (IOException e) {
            fail(e);
        }
        //Deleting temporary file.
        java.io.File file = new java.io.File("DBMSprocessesTest.txt");
        if (!file.delete()) System.err.println("Failed to delete DBMSprocessesTest.txt");

        BPlusTree<String,Integer> tree2 = new Tree<>(table.getMaxEntriesPerPage());
        tree2 = tree2.bufferToTree(tree2.readTree(table.getIndexPath()), table);

        //Checking if all previously inserted keys are included in the new tree
        for (int i = 0; i < 400; i++) {
            String key = keysList[i];
            if(!tree2.isKey(key)){
                fail();
            }
        }
        //Randomly deleting 100 entries from table.
        int ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400);
            if(tree2.isKey(keysList[randInd])){
                try {
                    tree2 = DBMS.deletionProcess(table, tree2, keysList[randInd]);
                } catch (IllegalArgumentException | IOException e) {
                    fail(e);
                }
                ind++;
            }
        }
        tree2.writeTree(table.getIndexPath(), tree2.treeToBuffer(tree2, table.getMaxIDSize()));
    }

    //@Test
    //@Order(5)
    void testOrderedEntryInsertion() {
        DBMS.dropTable(databaseName, tableName);
        DBMS.createTable(databaseName, tableName, null);
        
        tree = tree.bufferToTree(tree.readTree(table.getIndexPath()), table);
        int ind = 0;
        while(ind < 1000000){
            ArrayList<Object> entryData = new ArrayList<>();
            String key = "INSERTION"+ind;
            entryData.add(key);
            entryData.add(ind);
            entryData.add("TEST");
            entryData.add(new byte[] {(byte)ind});
            Entry<String> entry = new Entry<>(entryData, table.getMaxIDSize());
            entry.setID(table.getIDindex());
            try {
                tree = DBMS.insertionProcess(table, entry, tree);
            } catch (IOException e) {
                fail(e);
            }
            ind++;
        }
    }

    @AfterAll
    static void end(){
        new DBMSprocesses<>().dropTable(databaseName, tableName);
    }
}
