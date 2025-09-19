package com.database.db.CRUD;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.database.db.api.Condition.*;
import com.database.db.api.DBMS.*;
import com.database.db.api.Schema;
import com.database.db.manager.EntryManager;
import com.database.db.page.Entry;
import com.database.db.Database;
import com.database.db.table.DataType;
import com.database.db.table.Table;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntryManagerTest {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = SECURE_RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

    private Database database;
    private Table table;
    private TableConfig config;
    private EntryManager CRUD;
    private Random random;
    private String[] keysList;

    @BeforeAll
    void setup() throws ExecutionException, InterruptedException, IOException, Exception {
        database = new Database("test_database",null,100);
        
        Schema schema = new Schema()
            .column("username").type(DataType.CHAR).size(50).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn();

        config = new TableConfig("test_table", schema);
        database.createTable(config);
        database.create();
        table = database.getTable("test_table");
        
        CRUD = new EntryManager();
        CRUD.selectDatabase(database);
        CRUD.selectTable("test_table");

        random = new Random();
        keysList = new String[400];
    }

    @Test
    @Order(1)
    void testOneInsert() throws ExecutionException, InterruptedException, IOException, Exception {
        ArrayList<Object> entryData = new ArrayList<>();
        entryData.add("firstEntry");
        entryData.add(100);
        entryData.add("HELLO");
        byte[] data = new byte[10];
        entryData.add(data);
        Entry entry = new Entry(entryData.toArray(), table.getSchema().getNumOfColumns()).setBitMap(table.getSchema().getNotNull());
        CRUD.insertEntry(entry);
    }
    @Test
    @Order(2)
    void testDeleteLastEntry() throws ExecutionException, InterruptedException, IOException {
        WhereClause clause = new WhereClause().column("username")
            .isEqual("firstEntry").end();
        CRUD.deleteEntry(clause,-1);
    }

    @Test
    @Order(3)
    void testRandomEntryInsertion() throws ExecutionException, InterruptedException, IOException, Exception {
        int ind = 0;
        while (ind < 400) {
            int sizeOfID = random.nextInt(table.getSchema().getSizes()[table.getSchema().getPrimaryKeyIndex()]-1)+1;
            String userName = this.generateRandomString(sizeOfID);
            if(!table.isKeyFound(userName,0)){
                keysList[ind] = userName;
                ArrayList<Object> entryData = new ArrayList<>();
                entryData.add(userName);

                int intNum = random.nextInt();
                entryData.add(intNum);
                int sizeOfString = random.nextInt(table.getSchema().getSizes()[2]);
                String randStr = generateRandomString(sizeOfString);
                entryData.add(randStr);

                int sizeOfData = random.nextInt(table.getSchema().getSizes()[3]);
                byte[] data = new byte[sizeOfData];
                for(int y = 0; y < sizeOfData; y++){
                    data[y] = (byte) random.nextInt(127);
                }
                entryData.add(data);
                Entry entry = new Entry(entryData.toArray(), table.getSchema().getNumOfColumns()).setBitMap(table.getSchema().getNotNull());
                CRUD.insertEntry(entry);
                ind++;
            }
        }
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
    void testRandomDeletion() throws ExecutionException, InterruptedException, IOException {
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

        //Checking if all previously inserted keys are included in the new tree
        for (int i = 0; i < 400; i++) {
            String key = keysList[i];
            if(!table.isKeyFound(key,0)){
                fail();
            }
        }
        //Randomly deleting 100 entries from table.
        int ind = 0;
        while (ind < 100) {
            int randInd = random.nextInt(400);
            if(table.isKeyFound(keysList[randInd],0)){
                String key = keysList[randInd];
                WhereClause clause = new WhereClause().column("username")
                    .isEqual(key).end();
                CRUD.deleteEntry(clause,-1);
                ind++;
            }
        }
    }

    @Test
    @Order(5)
    void testOrderedEntryInsertion() throws ExecutionException, InterruptedException, IOException, Exception {
        int ind = 0;
        while(ind < 1000000){
            ArrayList<Object> entryData = new ArrayList<>();
            String key = "INSERTION"+ind;
            entryData.add(key);
            entryData.add(ind);
            entryData.add("TEST");
            entryData.add(new byte[] {(byte)ind});
            Entry entry = new Entry(entryData.toArray(), table.getSchema().getNumOfColumns()).setBitMap(table.getSchema().getNotNull());
            CRUD.insertEntry(entry);
            ind++;
        }
        database.commit();
        database.startTransaction("Million Deletions");
            CRUD.deleteEntry(null, -1);
            List<Entry> preRollbackResult = CRUD.selectEntriesAscending(null, 0, -1);
            assertEquals(0, preRollbackResult.size());
        database.rollBack();
        List<Entry> afterRollbackResult = CRUD.selectEntriesAscending(null, 0, -1);
        assertEquals(1000000, afterRollbackResult.size());
        CRUD.deleteEntry(null, -1);
        database.commit();
        List<Entry> commitDeletionsResult = CRUD.selectEntriesAscending(null, 0, -1);
        assertEquals(0, commitDeletionsResult.size());
    }

    @AfterAll
    void end(){
        try {
            table.getFileIOThread().shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        //database.removeAllTables();
    }
}