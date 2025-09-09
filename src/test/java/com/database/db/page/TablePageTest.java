package com.database.db.page;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;

import com.database.db.Database;
import com.database.db.FileIOThread;
import com.database.db.api.Schema;
import com.database.db.api.DBMS.TableConfig;
import com.database.db.table.DataType;
import com.database.db.table.Table;

import java.io.File;
import java.util.ArrayList;

class TablePageTest {
    private FileIOThread fileIOThread;
    private Table table;
    private TablePage page;
    private Entry entry1, entry2, entry3;
    private static final Schema SCHEMA = new Schema()
            .column("username").type(DataType.CHAR).size(50).primaryKey().endColumn()
            .column("num").type(DataType.INT).index().endColumn()
            .column("message").type(DataType.CHAR).size(10).endColumn()
            .column("data").type(DataType.BYTE).size(10).notNull().defaultValue(new byte[10]).endColumn();
            

    @BeforeEach
    void setUp() throws Exception {
        File file = new File("./testdb.test.table");
        File file1 = new File("./testdb.test.username.index");
        File file2 = new File("./testdb.test.num.index");
        file.createNewFile();
        file1.createNewFile();
        file2.createNewFile();
        fileIOThread = new FileIOThread();
        fileIOThread.start();
        TableConfig config = new TableConfig("test", SCHEMA, null);
        Database database = new Database("testdb");
        database.createTable(config);
        table = database.getTable("test");
        database.create();

        entry1 = createEntry("user1", 100, "msg1", new byte[]{1,2,3});
        entry2 = createEntry("user22", 200, "msg22", new byte[]{4,5,6});
        entry3 = createEntry("user333", 300, "msg333", new byte[]{7,8,9});
        page = new TablePage(0, table);
    }

    private Entry createEntry(String username, int num, String message, byte[] data) {
        ArrayList<Object> entryData = new ArrayList<>();
        entryData.add(username);
        entryData.add(num);
        entryData.add(message);
        entryData.add(data);
        Entry entry = new Entry(entryData.toArray(), 0).setBitMap(table.getSchema().getNotNull());
        return entry;
    }

    // Test basic operations
    @Test
    void addEntry_IncreasesSizeAndSpaceUsage() {
        emptyPage();
        page.add(entry1);
        assertEquals(1, page.size());
        assertEquals(TablePage.sizeOfEntry(table), page.getSpaceInUse());
        assertTrue(page.contains(entry1));
    }

    @Test
    void addMultipleEntries_MaintainsSortedOrder() {
        emptyPage();
        page.add(entry3);
        page.add(entry1);
        page.add(entry2);
        
        assertEquals(3, page.size());
        assertEquals(entry1, page.get(1));
        assertEquals(entry2, page.get(2));
        assertEquals(entry3, page.get(0));
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
        assertEquals(initialSpace - TablePage.sizeOfEntry(table), page.getSpaceInUse());
        assertEquals(entry2, page.get(0));
    }

    @Test
    void removeByKey_RemovesCorrectEntry() {
        emptyPage();
        page.add(entry1);
        page.add(entry2);
        int ind = page.indexOf(entry1);
        page.remove(ind);
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
        table.getCache().tableCache.writePage(page);
        
        TablePage newPage = table.getCache().tableCache.get(0);
        //newPage.bufferToPage(buffer, table);
        
        assertEquals(page.size(), newPage.size());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        arePageEntriesEqual(page,newPage);
    }

    @Test
    void bufferToPage_ThrowsForInvalidBufferSize() {
        byte[] invalidBuffer = new byte[512]; // Not multiple of 4096
        assertThrows(IllegalArgumentException.class, 
            () -> page.fromBytes(invalidBuffer));
    }

    // Test file I/O operations
    @Test
    void writeAndReadPage_ConsistentData() throws Exception {
        emptyPage();
        page.add(entry1);
        page.add(entry3);
        table.getCache().tableCache.writePage(page);
        
        TablePage newPage = table.getCache().tableCache.get(0);
        assertEquals(page.size(), newPage.size());
        assertEquals(page.getSpaceInUse(), newPage.getSpaceInUse());
        arePageEntriesEqual(page,newPage);
    }

    @Test
    void readNonExistentPage_CreatesEmptyPage() throws Exception {
        TablePage newPage = new TablePage(99, table); // Unwritten page
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
        assertTrue(stats.contains("Space in Use :            [ " + TablePage.sizeOfEntry(table)));
    }

    // Helper method
    private void fillPage() {
        for (int i = this.page.size(); i < Page.getPageCapacity(TablePage.sizeOfEntry(table)); i++) {
            Entry e = createEntry("user" + i, i, "msg", new byte[10]);
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
    private void arePageEntriesEqual(Page page1, Page page2){
        for (int i = 0; i < page1.size(); i++) {
            areEntriesEqual(page1.get(i), page2.get(i));
        }
    }
    private void areEntriesEqual(Entry entry1, Entry entry2){
        for (int i = 0; i < entry1.size();i++) {
            Object[] entry = entry1.getEntry();
            if(entry[i] instanceof String){
                assertEquals(entry[i], entry2.get(i));
            }else if(entry[i] instanceof Integer){
                assertEquals(entry[i], entry2.get(i));
            }else if(entry[i] instanceof byte[]){
                assertArrayEquals((byte[])entry[i],(byte[]) entry2.get(i));
            }else{
                fail("Invalid non sported type for entry.");
            }
        }
    }
    @AfterAll
    static void end(){
        File file = new File("./testdb.test.table");
        File file1 = new File("./testdb.test.username.index");
        File file2 = new File("./testdb.test.num.index");
        file.delete();
        file1.delete();
        file2.delete();
    }
}