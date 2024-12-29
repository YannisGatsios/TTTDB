package com.database.db.page;

import com.database.db.Entry;
import com.database.db.Schema;
import com.database.db.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class PageTest {

    private Table table;
    private Page page;
    private Entry entry1;
    private Entry entry2;
    private Entry entry3;

    @BeforeEach
    void setUp() {
        String databaseName = "system";
        String tableName = "users";
        Schema schema = new Schema("username:10:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false".split(";"));
        table = new Table(databaseName, tableName, schema);

        // Entry 1
        ArrayList<Object> entryData1 = new ArrayList<>();
        entryData1.add("johnttt1");
        entryData1.add(101);
        entryData1.add("hello");
        byte[] buffer1 = new byte[10];
        buffer1[9] = (byte) 0xff;
        entryData1.add(buffer1);
        entry1 = new Entry(entryData1, table.getMaxIDSize());
        entry1.setID(table.getIDindex());

        // Entry 2
        ArrayList<Object> entryData2 = new ArrayList<>();
        entryData2.add("johnttt22");
        entryData2.add(102);
        entryData2.add("hello");
        byte[] buffer2 = new byte[10];
        buffer2[9] = (byte) 0xff;
        entryData2.add(buffer2);
        entry2 = new Entry(entryData2, table.getMaxIDSize());
        entry2.setID(table.getIDindex());

        // Entry 3
        ArrayList<Object> entryData3 = new ArrayList<>();
        entryData3.add("johnttt333");
        entryData3.add(103);
        entryData3.add("hello");
        byte[] buffer3 = new byte[10];
        buffer3[9] = (byte) 0xff;
        entryData3.add(buffer3);
        entry3 = new Entry(entryData3, table.getMaxIDSize());
        entry3.setID(table.getIDindex());

        page = new Page(0, (short) table.getMaxEntriesPerPage());
        page.setMaxSizeOfEntry(table.getSizeOfEntry());
    }

    @Test
    void testAddEntry() {
        page.addEntry(entry1);
        page.addEntry(entry2);

        assertEquals(2, page.getNumOfEntries());
        assertEquals(entry1.size() + entry2.size(), page.getSpaceInUse());
        assertTrue(page.getEntries().contains(entry1));
        assertTrue(page.getEntries().contains(entry2));
    }

    @Test
    void testAddEntryThrowsExceptionWhenFull() {
        for (int i = 0; i < table.getMaxEntriesPerPage(); i++) {
            page.addEntry(entry1);
        }

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> page.addEntry(entry2));
        assertEquals("this Paage is full, current Max Size : " + page.getNumOfEntries(), exception.getMessage());
    }

    @Test
    void testRemoveEntry() {
        page.addEntry(entry1);
        page.addEntry(entry2);
        page.addEntry(entry3);

        int initialSpace = page.getSpaceInUse();
        page.removeEntry(1);

        assertEquals(2, page.getNumOfEntries());
        assertEquals(initialSpace - entry2.size(), page.getSpaceInUse());
        assertFalse(page.getEntries().contains(entry2));
    }

    @Test
    void testRemoveEntryThrowsExceptionWhenInvalidIndex() {
        page.addEntry(entry1);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> page.removeEntry(5));
        assertEquals("Invalid Number OF Entry to remove out of bounds yu=ou gave :5", exception.getMessage());
    }

    @Test
    void testPageStats() {
        page.addEntry(entry1);
        page.addEntry(entry2);

        String stats = page.pageStats();
        assertTrue(stats.contains("Page ID :                 0"));
        assertTrue(stats.contains("Number Of Entries :        2"));
        assertTrue(stats.contains("Space in Use :            [ " + page.getSpaceInUse() + "/" + page.sizeOfEntries() + " ]"));
    }

    @Test
    void testWriteAndReadPage() throws IOException {
        page.addEntry(entry1);
        page.addEntry(entry2);
        page.addEntry(entry3);

        byte[] data = page.pageToBuffer(page);
        String path = "storage/" + table.getDatabaseName() + "." + table.getTableName() + ".table";
        page.writePage(path, data, page.getPageID() * page.sizeOfPage());

        Page newPage = new Page(0, (short) table.getMaxEntriesPerPage());
        byte[] bufferPage = newPage.readPage(path, page.getPageID() * page.sizeOfPage(), page.sizeOfPage());
        newPage = newPage.bufferToPage(bufferPage, table);

        assertEquals(page.getNumOfEntries(), newPage.getNumOfEntries());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        assertEquals(page.getPageID(), newPage.getPageID());
    }
}
