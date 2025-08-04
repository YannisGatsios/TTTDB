package com.database.db.table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.database.db.api.DBMS.*;
import com.database.db.manager.EntryManager;
import com.database.db.Database;
import com.database.db.FileIOThread;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class TableTest {

    @TempDir
    Path tempDir;

    private Table table;
    private String testPath;
    private String schemaDef;
    private Database database;

    @BeforeEach
    void setUp() throws Exception {
        testPath = tempDir.toString() + File.separator;
        String databaseName = "testDB";
        String tableName = "testTable";
        schemaDef = "username:VARCHAR:10:PRIMARY_KEY:NULL;num:INT:4:INDEX:NULL";
        TableConfig tableConf = new TableConfig(tableName, schemaDef, 10);
        database = new Database(databaseName);
        database.setPath(testPath);
        database.createTable(tableConf);
        // Create real Schema instance

        // Get table
        table = database.getTable(tableName);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        database.close();
        database.removeAllTables();
    }

    @Test
    void tableInitialization_SetsCorrectProperties() {
        assertEquals("testDB", table.getDatabaseName());
        assertEquals("testTable", table.getName());
        assertEquals(testPath + "testDB.testTable.table", table.getPath());
    }

    @Test
    void getPageCapacity_CalculatesCorrectly() {
        // Expected calculation:
        // BLOCK_SIZE = 4096, SIZE_OF_HEADER = 2*4 + 2 = 10 bytes
        // Null bitmap size = (schema.numNullables()+7)/8 = (0+7)/8 = 1
        // Size of username: VARCHAR(10) = 10 + 2 = 12 bytes
        // Size of num: INT = 4 bytes
        // Total entry size = 1 + 12 + 4 = 17 bytes
        // Capacity = (4096 - 10) / 17 = 240
        assertEquals(240, table.getPageCapacity());
    }

    @Test
    void getIndexPath_GeneratesCorrectPath() {
        String path = table.getIndexPath(1);
        assertEquals(testPath + "testDB.testTable.num.index", path);
    }

    @Test
    void fileOperations_HandlePageCount() throws Exception {
        // Create test file with 3 blocks (12288 bytes)
        Path tablePath = tempDir.resolve("testDB.testTable.table");
        Files.write(tablePath, new byte[12288]);
        FileIOThread file = new FileIOThread();
        file.start();
        Table diskTable = new Table("testDB", "testTable", schemaDef, file, 10, testPath);
        assertEquals(3, diskTable.getPages());
        file.shutdown();
    }
    
    @Test
    void pageSizeCalculations_AreConsistent() {
        int calculatedPageSize = table.pageSizeInBytes();
        // Should be multiple of block size (4096)
        assertEquals(0, calculatedPageSize % 4096);
        assertTrue(calculatedPageSize > 0);
    }
    
    @Test
    void autoIncrementing_InitializesCorrectly() throws Exception {
        // Create schema with auto-increment column
        String aiSchemaDef = "id:LONG:4:AUTO_INCREMENT:NULL;name:VARCHAR:20:NO_CONSTRAINT:NULL";
        //Files.createFile(tempDir.resolve("testDB.aiTable.table"));
        TableConfig tableConf = new TableConfig("aiTable", aiSchemaDef, 10);
        database.createTable(tableConf);

        // Create table with auto-increment
        Table aiTable = database.getTable("aiTable");
        
        // Add some entries to build up the auto-increment sequence
        EntryManager crud = new EntryManager();
        crud.selectDatabase(database);
        crud.selectTable("aiTable");
        for (long i = 1; i <= 100; i++) {
            Object[] entryData = {i, "Name" + i};
            Entry entry = Entry.prepareEntry(new String[]{"id","name"}, entryData, aiTable);
            crud.insertEntry(entry);
        }
        
        // Verify auto-increment starts at the next value
        AutoIncrementing ai = aiTable.getAutoIncrementing(0);
        assertEquals(101L, ai.getNextKey());

        for (long i = 1; i <= 100; i++) {
            Object[] entryData = {"Name" + i};
            Entry entry = Entry.prepareEntry(new String[]{"name"}, entryData, aiTable);
            crud.insertEntry(entry);
        }
        
        // Verify auto-increment starts at the next value
        assertEquals(202L, ai.getNextKey());
    }
}