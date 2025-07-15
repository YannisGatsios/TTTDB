package com.database.db.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.db.FileIOThread;

class TableTest {

    private FileIOThread fileIOThread;
    private Schema validSchema;
    private Schema complexSchema;

    @BeforeEach
    void setup() {
        fileIOThread = new FileIOThread();
        fileIOThread.start();
        
        // Simple schema: id (INT, primary key) and name (STRING)
        validSchema = new Schema(new String[] {
            "id:4:Integer:false:false:true",
            "name:20:String:false:false:false"
        });
        
        // Complex schema from your main example
        complexSchema = new Schema(
           ("username:10:String:false:false:true;" +
            "num:4:Integer:false:true:false;" +
            "message:5:String:true:true:false;" +
            "data:10:Byte:false:false:false").split(";")
        );
    }

    @Test
    void testConstructorWithSimpleSchema() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread);
        
        assertEquals("testdb", table.getDatabaseName());
        assertEquals("users", table.getName());
        assertEquals(2, table.getNumOfColumns());
        assertEquals(4 + 20, table.getSizeOfEntry());
        assertEquals(0, table.getPrimaryKeyColumnIndex());
        assertEquals(Type.INT, table.getPrimaryKeyType());
        assertEquals("storage/testdb.users.table", table.getPath());
        assertEquals("storage/index/testdb.users.index", table.getPKPath());
    }

    @Test
    void testValidEntry() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread);
        // Valid entry
        ArrayList<Object> data = new ArrayList<>(Arrays.asList(42, "John Doe"));
        Entry entry = new Entry(data, validSchema.getSizes()[0]);// Set ID to first column value
        assertTrue(table.isValidEntry(entry));
    }

    @Test
    void testInvalidEntry() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread);
        // Wrong column count
        ArrayList<Object> invalidData1 = new ArrayList<>(Arrays.asList(42));
        Entry invalidEntry1 = new Entry(invalidData1, validSchema.getSizes()[0]);
        assertFalse(table.isValidEntry(invalidEntry1));
    }

    @Test
    void testPageCapacityCalculation() throws Exception {
        // Test large column (4000 bytes)
        Schema largeColSchema = new Schema(new String[]{"string:4000:String:false:false:true"});
        Table table1 = new Table("testdb", "large", largeColSchema, fileIOThread);
        assertEquals(3, table1.getEntriesPerPage());
    }

    @Test
    void testPageManagement() throws Exception {
        Table table = new Table("testDB", "users", validSchema, fileIOThread);
        
        assertEquals(0, table.getPages());
        table.addOnePage();
        assertEquals(1, table.getPages());
        table.removeOnePage();
        assertEquals(0, table.getPages());
    }

    @Test
    void testConstructorWithComplexSchema() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread);

        assertEquals("system", table.getDatabaseName());
        assertEquals("users", table.getName());
        assertEquals(4, table.getNumOfColumns());
        assertEquals(10 + 4 + 5 + 10, table.getSizeOfEntry());
        assertEquals(0, table.getPrimaryKeyColumnIndex()); // username is primary key
        assertEquals(Type.STRING, table.getPrimaryKeyType());
    }

    @Test
    void testComplexSchemaSizeCalculation() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread);
        // username:10 + num:4 + message:5 + data:10 = 29 bytes
        assertEquals(29, table.getSizeOfEntry());
    }

    @Test
    void testComplexSchemaPageCapacity() throws Exception {
        Table table = new Table("system", "users", complexSchema, fileIOThread);
        // Block size 4096 - header 12 bytes = 4084
        // 4084 / 29 ≈ 140 entries
        assertTrue(table.getEntriesPerPage() >= 140);
    }

    @Test
    void testGetPrimaryKeyType() throws Exception {
        Table intTable = new Table("testdb", "intTable", validSchema, fileIOThread);
        assertEquals(Type.INT, intTable.getPrimaryKeyType());

        Table stringTable = new Table("test", "stringTable", complexSchema, fileIOThread);
        assertEquals(Type.STRING, stringTable.getPrimaryKeyType());
    }

    @Test
    void testPageManagementMultiplePages() throws Exception {
        Table table = new Table("testdb", "users", validSchema, fileIOThread);

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
        Table table = new Table("testdb", "users", complexSchema, fileIOThread);

        // Valid complex entry
        ArrayList<Object> data = new ArrayList<>(Arrays.asList(
                "user123", // username (primary key)
                42, // num (Integer)
                "hello", // message (String)
                new byte[10] // data (Byte array)
        ));
        Entry entry = new Entry(data, complexSchema.getSizes()[0]);
        assertTrue(table.isValidEntry(entry));
    }

    @Test
    void testInvalidComplexEntry() throws Exception {
        Table table = new Table("testdb", "users", complexSchema, fileIOThread);

        // Invalid: Wrong type for num (String instead of Integer)
        ArrayList<Object> invalidData = new ArrayList<>(Arrays.asList(
                "user123",
                "should-be-int", // Invalid type
                "hello",
                new byte[10]));
        Entry entry = new Entry(invalidData, complexSchema.getSizes()[0]);
        assertThrows(IllegalArgumentException.class, () -> table.isValidEntry(entry));
    }

    @Test
    void testEntryWithWrongSize() throws Exception {
        Table table = new Table("testdb", "users", complexSchema, fileIOThread);

        // Invalid: Username too long (max 10 chars)
        ArrayList<Object> invalidData = new ArrayList<>(Arrays.asList(
                "this_username_is_too_long", // 24 chars > 10
                42,
                "hello",
                new byte[10]));
        Entry entry = new Entry(invalidData, complexSchema.getSizes()[0]);
        assertThrows(IllegalArgumentException.class, () -> table.isValidEntry(entry));
    }

    @Test
    void testNullableFieldValidation() throws Exception {
        // Create schema where message is nullable
        Schema nullableSchema = new Schema(
                ("username:10:String:false:false:true;" +
                        "message:5:String:true:true:false").split(";"));

        Table table = new Table("testdb", "nullable", nullableSchema, fileIOThread);

        // Valid null entry
        ArrayList<Object> validNull = new ArrayList<>(Arrays.asList("user1", null));
        Entry entry = new Entry(validNull, nullableSchema.getSizes()[0]);
        assertTrue(table.isValidEntry(entry));

        // Invalid: Non-nullable field is null
        ArrayList<Object> invalidNull = new ArrayList<>(Arrays.asList(null, "msg"));
        Entry invalidEntry = new Entry(invalidNull, nullableSchema.getSizes()[0]);
        assertThrows(Exception.class, () -> table.isValidEntry(invalidEntry));
    }
    
}