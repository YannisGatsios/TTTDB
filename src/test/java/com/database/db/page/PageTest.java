package com.database.db.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class PageTest {

    private FileIOThread fileIOThread = new FileIOThread();
    private static FileIO fileIO;
    private Table table;
    private Page<String> page;
    private Entry<String> entry1;
    private Entry<String> entry2;
    private Entry<String> entry3;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException{
        fileIOThread.start();
        fileIO = new FileIO(fileIOThread);
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        table = new Table(databaseName, tableName, schema, fileIOThread);

        // Entry 1
        ArrayList<Object> entryData1 = new ArrayList<>();
        entryData1.add("johnttt1");
        entryData1.add(101);
        entryData1.add("hello");
        byte[] buffer1 = new byte[10];
        buffer1[9] = (byte) 0xff;
        entryData1.add(buffer1);
        entry1 = new Entry<>(entryData1, table.getPrimaryKeyMaxSize());
        entry1.setID(table.getPrimaryKeyColumnIndex());

        // Entry 2
        ArrayList<Object> entryData2 = new ArrayList<>();
        entryData2.add("johnttt22");
        entryData2.add(102);
        entryData2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entryData2.add(buffer2);
        entry2 = new Entry<>(entryData2, table.getPrimaryKeyMaxSize());
        entry2.setID(table.getPrimaryKeyColumnIndex());

        // Entry 3
        ArrayList<Object> entryData3 = new ArrayList<>();
        entryData3.add("johnttt333");
        entryData3.add(103);
        entryData3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entryData3.add(buffer3);
        entry3 = new Entry<>(entryData3, table.getPrimaryKeyMaxSize());
        entry3.setID(table.getPrimaryKeyColumnIndex());

        page = new Page<>(0, table);
    }

    @Test
    void testAddEntry() {
        page.add(entry1);
        page.add(entry2);

        assertEquals(2, page.size());
        assertEquals(entry1.size() + entry2.size(), page.getSpaceInUse());
        assertTrue(page.getAll().contains(entry1));
        assertTrue(page.getAll().contains(entry2));
    }

    @Test
    void testAddEntryThrowsExceptionWhenFull() {
        for (int i = 0; i < table.getPageMaxNumOfEntries(); i++) {
            page.add(entry1);
        }

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> page.add(entry2));
        assertEquals("this Page is full, current Max Size : " + page.size(), exception.getMessage());
    }

    @Test
    void testRemoveEntry() {
        page.add(entry1);
        page.add(entry2);
        page.add(entry3);

        int initialSpace = page.getSpaceInUse();
        page.remove(1);

        assertEquals(2, page.size());
        assertEquals(initialSpace - entry2.size(), page.getSpaceInUse());
        assertFalse(page.getAll().contains(entry2));
    }

    @Test
    void testRemoveEntryThrowsExceptionWhenInvalidIndex() {
        page.add(entry1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> page.remove(5));
        assertEquals("Invalid Number OF Entry to remove from Page out of bounds you gave : 5", exception.getMessage());
    }

    @Test
    void testPageStats() {
        page.add(entry1);
        page.add(entry2);

        String stats = page.toString();
        assertTrue(stats.contains("Page ID :                 0"));
        assertTrue(stats.contains("Number Of Entries :        2"));
        assertTrue(stats.contains("Space in Use :            [ " + page.getSpaceInUse() + "/" + page.sizeOfEntries() + " ]"));
    }

    @Test
    void testWriteAndReadPage() throws IOException, ExecutionException, InterruptedException {
        page.add(entry1);
        page.add(entry2);
        page.add(entry3);

        byte[] data = page.pageToBuffer(page);
        String path = "storage/" + table.getDatabaseName() + "." + table.getTableName() + ".table";
        fileIO.writePage(path, data, page.getPageID() * page.sizeInBytes());

        Page<String> newPage = new Page<>(0, table);
        byte[] bufferPage = fileIO.readPage(path, page.getPageID() * page.sizeInBytes(), page.sizeInBytes());
        newPage = newPage.bufferToPage(bufferPage, table);

        assertEquals(page.size(), newPage.size());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        assertEquals(page.getPageID(), newPage.getPageID());
    }
}
