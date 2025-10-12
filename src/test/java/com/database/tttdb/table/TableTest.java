package com.database.tttdb.table;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.database.tttdb.api.Schema;
import com.database.tttdb.api.DBMS.*;
import com.database.tttdb.core.Database;
import com.database.tttdb.core.FileIOThread;
import com.database.tttdb.core.page.Entry;
import com.database.tttdb.core.page.Page;
import com.database.tttdb.core.page.TablePage;
import com.database.tttdb.core.table.DataType;
import com.database.tttdb.core.table.Table;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class TableTest {

    @TempDir
    Path tempDir;

    private Table table;
    private String testPath;
    private Database database;

    @BeforeEach
    void setUp() throws Exception {
        testPath = tempDir.toString() + File.separator;
        String databaseName = "testDB";
        String tableName = "testTable";
        
        TableConfig tableConf = new TableConfig(tableName, new Schema()
            .column("username").type(DataType.CHAR).size(10).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn());
        database = new Database(databaseName,null,10);
        database.setPath(testPath);
        database.createTable(tableConf);
        database.start();
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
        assertEquals(240, Page.getPageCapacity(TablePage.sizeOfEntry(table)));
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
        FileIOThread file = new FileIOThread("testThread");
        file.start();
        TableConfig config = new TableConfig("testTable", new Schema()
            .column("username").type(DataType.CHAR).size(10).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn());
        Database db = new Database("testDB",null,10);
        db.setPath(testPath);
        db.createTable(config);
        db.start();
        Table diskTable = db.getTable("testTable");
        assertEquals(3, diskTable.getPages());
        file.shutdown();
    }
    
    @Test
    void pageSizeCalculations_AreConsistent() {
        int calculatedPageSize = Page.pageSizeInBytes(TablePage.sizeOfEntry(table));
        // Should be multiple of block size (4096)
        assertEquals(0, calculatedPageSize % 4096);
        assertTrue(calculatedPageSize > 0);
    }
    
    @Test
    void autoIncrementing_InitializesCorrectly() throws Exception {
        database.close();
        // Create schema with auto-increment column
        TableConfig config = new TableConfig("aiTable", new Schema()
            .column("id").autoIncrementing().endColumn()
            .column("name").type(DataType.CHAR).size(20).endColumn());
        database.createTable(config);
        database.start();

        // Create table with auto-increment
        Table aiTable = database.getTable("aiTable");
        
        // Add some entries to build up the auto-increment sequence
        for (long i = 1; i <= 100; i++) {
            Object[] entryData = {i, "Name" + i};
            Entry entry = Entry.prepareEntry(new String[]{"id","name"}, entryData, aiTable);
            aiTable.insertUnsafe(entry);
        }
        
        // Verify auto-increment starts at the next value
        assertEquals(101L, aiTable.nextAutoIncrementValue(0));

        for (long i = 1; i <= 100; i++) {
            Object[] entryData = {"Name" + i};
            Entry entry = Entry.prepareEntry(new String[]{"name"}, entryData, aiTable);
            aiTable.insertUnsafe(entry);
        }
        
        // Verify auto-increment starts at the next value
        assertEquals(202L, aiTable.nextAutoIncrementValue(0));
    }
}