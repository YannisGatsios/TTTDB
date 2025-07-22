package com.database.db.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.db.FileIOThread;

class TableTest {

    private FileIOThread fileIOThread;
    private Schema validSchema;
    private Schema complexSchema;

    @BeforeEach
    void setup() throws Exception{
        fileIOThread = new FileIOThread();
        fileIOThread.start();
        
        // Simple schema: id (INT, primary key) and name (STRING)
        validSchema = new Schema(new String[] {
            "id:INT:NON:PRIMARY_KEY:NULL;",
            "username:VARCHAR:20:NO_CONSTRAINT:NULL;"
        });
        
        // Complex schema from your main example
        complexSchema = new Schema((
            "username:VARCHAR:10:PRIMARY_KEY:NULL;"+
            "num:INT:NON:INDEX:NULL;"+
            "message:VARCHAR:10:NO_CONSTRAINT:NULL;"+
            "data:BINARY:10:NOT_NULL:NON")
            .split(";"));
    }

    @Test
    void testConstructorWithSimpleSchema() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread,"storage/");
        
        assertEquals("testdb", table.getDatabaseName());
        assertEquals("users", table.getName());
        assertEquals(2, table.getSchema().getNumOfColumns());
        assertEquals(4 + 20, table.getSizeOfEntry());
        assertEquals(0, table.getSchema().getPrimaryKeyIndex());
        assertEquals(DataType.INT, table.getSchema().getTypes()[table.getSchema().getPrimaryKeyIndex()]);
        assertEquals("storage/testdb.users.table", table.getPath());
        assertEquals("storage/testdb.users.username.index", table.getIndexPath(0));
    }

    @Test
    void testValidEntry() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread,"storage/");
        // Valid entry
        Object[] data = {42, "John Doe"};
        Entry entry = new Entry(data, table);// Set ID to first column value
        assertTrue(Entry.isValidEntry(entry.getEntry(), table.getSchema()));
    }

    @Test
    void testInvalidEntry() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread,"storage/");
        // Wrong column count
        Object[] invalidData1 = {42};
        Entry invalidEntry1 = new Entry(invalidData1, table);
        assertFalse(Entry.isValidEntry(invalidEntry1.getEntry(), table.getSchema()));
    }

    @Test
    void testPageCapacityCalculation() throws Exception {
        // Test large column (4000 bytes)
        Schema largeColSchema = new Schema(new String[]{"string:4000:String:false:false:true"});
        Table table1 = new Table("testdb", "large", largeColSchema, fileIOThread,"storage/");
        assertEquals(3, table1.getPageCapacity());
    }

    @Test
    void testPageManagement() throws Exception {
        Table table = new Table("testDB", "users", validSchema, fileIOThread,"storage/");
        
        assertEquals(0, table.getPages());
        table.addOnePage();
        assertEquals(1, table.getPages());
        table.removeOnePage();
        assertEquals(0, table.getPages());
    }

    @Test
    void testConstructorWithComplexSchema() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread,"storage/");

        assertEquals("system", table.getDatabaseName());
        assertEquals("users", table.getName());
        assertEquals(4, table.getSchema().getNumOfColumns());
        assertEquals(10 + 4 + 5 + 10, table.getSizeOfEntry());
        assertEquals(0, table.getSchema().getPrimaryKeyIndex()); // username is primary key
        assertEquals(DataType.VARCHAR, table.getSchema().getTypes()[table.getSchema().getPrimaryKeyIndex()]);
    }

    @Test
    void testComplexSchemaSizeCalculation() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread,"storage/");
        // username:10 + num:4 + message:5 + data:10 = 29 bytes
        assertEquals(29, table.getSizeOfEntry());
    }

    @Test
    void testComplexSchemaPageCapacity() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread,"storage/");
        // Block size 4096 - header 12 bytes = 4084
        // 4084 / 29 ≈ 140 entries
        assertTrue(table.getPageCapacity() >= 140);
    }

    @Test
    void testGetPrimaryKeyType() throws Exception {
        Table intTable = new Table("testdb", "intTable", validSchema, fileIOThread,"storage/");
        assertEquals(DataType.INT, intTable.getSchema().getTypes()[intTable.getSchema().getPrimaryKeyIndex()]);

        Table stringTable = new Table("test", "stringTable", complexSchema, fileIOThread,"storage/");
        assertEquals(DataType.VARCHAR, stringTable.getSchema().getTypes()[stringTable.getSchema().getPrimaryKeyIndex()]);
    }

    @Test
    void testPageManagementMultiplePages() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread,"storage/");

        assertEquals(0, table.getPages());
        table.addOnePage();
        table.addOnePage();
        table.addOnePage();
        assertEquals(3, table.getPages());

        table.removeOnePage();
        assertEquals(2, table.getPages());

        table.removeOnePage();
        table.removeOnePage();
        assertEquals(0, table.getPages());
    }

    @Test
    void testValidComplexEntry() throws Exception {
        Table table = new Table("testdb", "users", complexSchema, fileIOThread,"storage/");

        // Valid complex entry
        Object[] data = {
                "user123", // username (primary key)
                42, // num (Integer)
                "hello", // message (String)
                new byte[10] // data (Byte array)
        };
        Entry entry = new Entry(data, table);
        assertTrue(Entry.isValidEntry(entry.getEntry(), table.getSchema()));
    }

    @Test
    void testInvalidComplexEntry() throws Exception {
        Table table = new Table("testdb", "users", complexSchema, fileIOThread,"storage/");

        // Invalid: Wrong type for num (String instead of Integer)
        Object[] invalidData = {
                "user123",
                "should-be-int", // Invalid type
                "hello",
                new byte[10]};
        Entry entry = new Entry(invalidData, table);
        assertThrows(IllegalArgumentException.class, () -> Entry.isValidEntry(entry.getEntry(), table.getSchema()));
    }

    @Test
    void testEntryWithWrongSize() throws Exception {
        Table table = new Table("testdb", "users", complexSchema, fileIOThread,"storage/");

        // Invalid: Username too long (max 10 chars)
        Object[] invalidData = {
                "this_username_is_too_long", // 24 chars > 10
                42,
                "hello",
                new byte[10]};
        Entry entry = new Entry(invalidData, table);
        assertThrows(IllegalArgumentException.class, () -> Entry.isValidEntry(entry.getEntry(), table.getSchema()));
    }

    @Test
    void testNullableFieldValidation() throws Exception {
        // Create schema where message is nullable
        Schema nullableSchema = new Schema(
                ("username:10:String:false:false:true;" +
                        "message:5:String:true:true:false").split(";"));

        Table table = new Table("testdb", "nullable", nullableSchema, fileIOThread,"storage/");

        // Valid null entry
        Object[] validNull = {"user1", null};
        Entry entry = new Entry(validNull, table);
        assertTrue(Entry.isValidEntry(entry.getEntry(), table.getSchema()));

        // Invalid: Non-nullable field is null
        Object[] invalidNull = {null, "msg"};
        Entry invalidEntry = new Entry(invalidNull, table);
        assertThrows(Exception.class, () -> Entry.isValidEntry(invalidNull,table.getSchema()));
    }
    
}