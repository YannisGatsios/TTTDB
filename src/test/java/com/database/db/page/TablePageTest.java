package com.database.db.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;

import com.database.db.FileIOThread;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

class TablePageTest {
    private FileIOThread fileIOThread;
    private Table<String> table;
    private TablePage<String> page;
    private Entry<String> entry1, entry2, entry3;
    private static final String SCHEMA_STRING = 
        "username:10:String:false:false:true;num:4:Integer:false:true:false;message:5:String:true:true:false;data:10:Byte:false:false:false";

    @BeforeEach
    void setUp() throws Exception {
        File file = new File("./storage/testdb.test.table");
        file.createNewFile();
        fileIOThread = new FileIOThread();
        fileIOThread.start();
        Schema schema = new Schema(SCHEMA_STRING.split(";"));
        table = new Table<>("testdb", "test", schema, fileIOThread);

        entry1 = createEntry("user1", 100, "msg1", new byte[]{1,2,3});
        entry2 = createEntry("user22", 200, "msg22", new byte[]{4,5,6});
        entry3 = createEntry("user333", 300, "msg333", new byte[]{7,8,9});
        page = new TablePage<>(0, table);
    }

    private Entry<String> createEntry(String username, int num, String message, byte[] data) {
        ArrayList<Object> entryData = new ArrayList<>();
        entryData.add(username);
        entryData.add(num);
        entryData.add(message);
        entryData.add(data);
        Entry<String> entry = new Entry<>(entryData, table.getPrimaryKeyMaxSize());
        entry.setID(table.getPrimaryKeyColumnIndex());
        return entry;
    }

    // Test basic operations
    @Test
    void addEntry_IncreasesSizeAndSpaceUsage() {
        emptyPage();
        page.add(entry1);
        assertEquals(1, page.size());
        assertEquals(entry1.size(), page.getSpaceInUse());
        assertTrue(page.getAll().contains(entry1));
    }

    @Test
    void addMultipleEntries_MaintainsSortedOrder() {
        emptyPage();
        page.add(entry3);
        page.add(entry1);
        page.add(entry2);
        
        assertEquals(3, page.size());
        assertEquals(entry1, page.get(0));
        assertEquals(entry2, page.get(1));
        assertEquals(entry3, page.get(2));
    }

    @Test
    void addEntry_ThrowsWhenPageFull() {
        fillPage();
        assertThrows(IllegalArgumentException.class, () -> page.add(entry1));
    }

    // Test removal operations
    @Test
    void removeByIndex_DecreasesSizeAndUpdatesSpace() {
        emptyPage();
        page.add(entry1);
        page.add(entry2);
        int initialSpace = page.getSpaceInUse();
        
        page.remove(0);
        assertEquals(1, page.size());
        assertEquals(initialSpace - entry1.size(), page.getSpaceInUse());
        assertEquals(entry2, page.get(0));
    }

    @Test
    void removeByKey_RemovesCorrectEntry() {
        emptyPage();
        page.add(entry1);
        page.add(entry2);
        page.remove(entry1.getID());
        assertEquals(1, page.size());
        assertEquals(entry2, page.get(0));
    }

    @Test
    void removeByIndex_ThrowsForInvalidIndex() {
        emptyPage();
        page.add(entry1);
        assertThrows(IllegalArgumentException.class, () -> page.remove(-1));
        assertThrows(IllegalArgumentException.class, () -> page.remove(1));
    }

    // Test serialization/deserialization
    @Test
    void toBufferAndBack_RestoresIdenticalPage() throws Exception {
        emptyPage();
        page.add(entry1);
        page.add(entry2);
        page.write(table);;
        
        TablePage<String> newPage = new TablePage<>(0, table);
        //newPage.bufferToPage(buffer, table);
        
        assertEquals(page.size(), newPage.size());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        arePageEntriesEqual(page,newPage);
    }

    @Test
    void bufferToPage_ThrowsForInvalidBufferSize() {
        byte[] invalidBuffer = new byte[512]; // Not multiple of 4096
        assertThrows(IllegalArgumentException.class, 
            () -> page.bufferToPage(invalidBuffer));
    }

    @Test
    void bufferToPage_ThrowsForCorruptedHeader() {
        byte[] buffer = new byte[4096];
        assertThrows(IOException.class, 
            () -> page.bufferToPage(buffer));
    }

    // Test file I/O operations
    @Test
    void writeAndReadPage_ConsistentData() throws Exception {
        emptyPage();
        page.add(entry1);
        page.add(entry3);
        page.write(table);
        
        TablePage<String> newPage = new TablePage<>(0, table);
        assertEquals(page.size(), newPage.size());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        arePageEntriesEqual(page,newPage);
    }

    @Test
    void readNonExistentPage_CreatesEmptyPage() throws Exception {
        TablePage<String> newPage = new TablePage<>(99, table); // Unwritten page
        assertEquals(0, newPage.size());
        assertEquals(0, newPage.getSpaceInUse());
    }

    // Test edge cases
    @Test
    void removeLastFromEmptyPage_ThrowsException() {
        emptyPage();
        assertThrows(IllegalArgumentException.class, () -> page.removeLast());
    }

    @Test
    void getEntryFromEmptyPage_ThrowsException() {
        emptyPage();
        assertThrows(IndexOutOfBoundsException.class, () -> page.get(0));
    }

    @Test
    void pageStats_FormatsCorrectly() {
        emptyPage();
        page.add(entry1);
        String stats = page.toString();
        
        assertTrue(stats.contains("Page ID :                 0"));
        assertTrue(stats.contains("Number Of Entries :        1"));
        assertTrue(stats.contains("Space in Use :            [ " + entry1.size()));
    }

    // Helper method
    private void fillPage() {
        for (int i = this.page.size(); i < table.getEntriesPerPage(); i++) {
            Entry<String> e = createEntry("user" + i, i, "msg", new byte[10]);
            page.add(e);
        }
    }
    private void emptyPage(){
        int size = page.size();
        while(size != 0){
            page.remove(size-1);
            size = page.size();
        }
    }
    private void arePageEntriesEqual(Page<String> page1, Page<String> page2){
        for (int i = 0; i < page1.size(); i++) {
            areEntriesEqual(page1.get(i), page2.get(i));
        }
    }
    private void areEntriesEqual(Entry<String> entry1, Entry<String> entry2){
        for (int i = 0; i < entry1.getEntry().size();i++) {
            ArrayList<Object> entry = entry1.getEntry();
            if(entry.get(i) instanceof String){
                assertEquals(entry.get(i), entry2.getEntry().get(i));
            }else if(entry.get(i) instanceof Integer){
                assertEquals(entry.get(i), entry2.getEntry().get(i));
            }else if(entry.get(i) instanceof byte[]){
                assertArrayEquals((byte[])entry.get(i),(byte[]) entry2.getEntry().get(i));
            }else{
                fail("Invalid non sported type for entry.");
            }
        }
    }
    @AfterAll
    static void end(){
        File file = new File("./storage/testdb.test.table");
        file.delete();
    }
}