package com.database.db.page;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.database.db.table.Table;
import com.database.db.FileIOThread;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Unit tests for the abstract Page class.
 * A concrete implementation (ConcretePage) and stubs for dependencies (MockTable, MockEntry)
 * are created to facilitate testing of the abstract class's logic.
 */
class PageTest {

    private static final int PAGE_ID = 1;

    private Table mockTable;
    private Page page;
    private Entry entry1;
    private Entry entry2;
    private Entry entry3;

    // --- Test Setup ---

    /**
     * A concrete implementation of the abstract Page class for testing purposes.
     * toBytes and fromBytes are implemented minimally as they are not the focus
     * of these abstract Page tests.
     */
    private static class ConcretePage extends Page {
        public ConcretePage(int PageID, Table table) {
            super(PageID, TablePage.sizeOfEntry(table));
        }

        @Override
        public byte[] toBytes() throws IOException {
            // Minimal implementation as per instruction, not relevant for abstract Page logic tests
            return new byte[0];
        }

        @Override
        public void fromBytes(byte[] bufferData) throws IOException {
            // Minimal implementation as per instruction, not relevant for abstract Page logic tests
        }
    }

    /**
     * A stub implementation of the Table class that mimics your actual Table's behavior
     * for size calculations, but without file I/O or index initialization.
     */
    private static class MockTable extends Table {
        public MockTable(String schemaConfig) throws IOException, ExecutionException, InterruptedException,Exception {
            super("mockDB", "mockTable", schemaConfig, new MockFileIOThread(), 10, "storage/");
        }

        // Mock FileIOThread to avoid actual file operations during tests
        private static class MockFileIOThread extends FileIOThread {
            public MockFileIOThread() {
                super(); // A dummy pool size
            }
            // No other FileIOThread methods are typically called by Table's constructors
            // for the purpose of setting up initial sizes, so this should be sufficient.
        }
    }

    /**
     * A stub implementation of the Entry class that now properly extends your new Entry constructor.
     */
    private static class MockEntry extends Entry {
        public MockEntry(Table table, Object... values) {
            super(values, table.getSchema().getNumOfColumns());
            this.setBitMap(table.getSchema().getNotNull()); // Call the Entry constructor with data and table
        }
        // Rely on Entry's sizeInBytes(), equals(), hashCode() implementations.
    }

    @BeforeEach
    void setUp() throws IOException, ExecutionException, InterruptedException, Exception {
        File file = new File("./mockDB.mockTable.table");
        file.createNewFile();
        String schemaConfig = 
    "id:INT:NON:NO_CONSTRAINT:NULL;" +
    "name:CHAR:50:NO_CONSTRAINT:NULL";

        mockTable = new MockTable(schemaConfig);
        page = new ConcretePage(PAGE_ID, mockTable);

        entry1 = new MockEntry(mockTable, 100, "Alice");
        entry2 = new MockEntry(mockTable, 200, "Bob");
        entry3 = new MockEntry(mockTable, 300, "Charlie");
    }

    // --- Test Cases ---

    @Test
    void testInitialization() {
        assertEquals(PAGE_ID, page.getPageID(), "Page ID should be set correctly.");
        assertEquals(0, page.size(), "Initial number of entries should be 0.");
        assertEquals(0, page.getSpaceInUse(), "Initial space in use should be 0.");
        assertFalse(page.isDirty(), "Page should not be dirty on initialization.");
        assertEquals(Page.getPageCapacity(TablePage.sizeOfEntry(mockTable)), page.getAll().length, "Entries array should be initialized to page capacity.");
    }

    @Test
    void testAddEntry() {
        int entrySize = TablePage.sizeOfEntry(mockTable);
        page.add(entry1);
        assertEquals(1, page.size());
        assertEquals(entrySize, page.getSpaceInUse());
        assertTrue(page.isDirty());
        assertSame(entry1, page.get(0));

        page.add(1, entry2);
        assertEquals(2, page.size());
        assertEquals(entrySize * 2, page.getSpaceInUse());
        assertSame(entry2, page.get(1));
    }

    @Test
    void testAddEntry_ThrowsExceptionWhenFull() {
        short pageCapacity = Page.getPageCapacity(TablePage.sizeOfEntry(mockTable));
        for (int i = 0; i < pageCapacity; i++) {
            page.add(new MockEntry(mockTable, i, "name" + i));
        }
        assertEquals(pageCapacity, page.size());

        Exception exception = assertThrows(IllegalArgumentException.class, () -> page.add(entry1));
        assertEquals("Index out of bounds: " + pageCapacity, exception.getMessage());
    }

    @Test
    void testAddEntry_ThrowsExceptionForNullEntry() {
        assertThrows(IllegalArgumentException.class, () -> page.add(null));
    }

    @Test
    void testAddEntry_ThrowsExceptionForExistingIndex() {
        page.add(entry1);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> page.add(0, entry2));
        assertEquals("Entry already exists at index 0 can not add a new one", exception.getMessage());
    }

    @Test
    void testRemoveEntryByIndex() {
        page.add(entry1);
        page.add(entry2);
        page.add(entry3);
        page.setDirty(false);

        int entrySize = TablePage.sizeOfEntry(mockTable);

        Entry removedEntry = page.remove(1);

        assertSame(entry2, removedEntry, "The correct entry should be returned on removal.");
        assertEquals(2, page.size(), "Number of entries should decrease.");
        assertEquals(entrySize * 2, page.getSpaceInUse(), "Space in use should decrease.");
        assertTrue(page.isDirty(), "Page should be marked dirty after removal.");

        assertSame(entry3, page.get(1), "Entry should be swapped with the last one.");
        assertNull(page.getAll()[2], "The last slot should be null after removal.");
    }

    @Test
    void testRemoveLastEntry() {
        page.add(entry1);
        page.add(entry2);

        Entry removedEntry = page.removeLast();
        assertSame(entry2, removedEntry);
        assertEquals(1, page.size());
        assertEquals(-1, page.indexOf(removedEntry));
        assertEquals(0, page.indexOf(entry1));
    }

    @Test
    void testRemove_ThrowsExceptionForInvalidIndex() {
        page.add(entry1);
        assertThrows(IllegalArgumentException.class, () -> page.remove(1));
        assertThrows(IllegalArgumentException.class, () -> page.remove(-1));
    }

    @Test
    void testGetEntry_ThrowsExceptionForInvalidIndex() {
        assertThrows(IndexOutOfBoundsException.class, () -> page.get(0));
        page.add(entry1);
        assertThrows(IndexOutOfBoundsException.class, () -> page.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> page.get(-1));
    }

    @Test
    void testContainsAndIndexOf() {
        page.add(entry1);
        page.add(entry2);

        assertTrue(page.contains(entry1));
        assertFalse(page.contains(entry3));
        assertEquals(0, page.indexOf(entry1));
        assertEquals(1, page.indexOf(entry2));
        assertEquals(-1, page.indexOf(entry3));
    }

    @Test
    void testSizingCalculations() {
        int actualEntrySize = TablePage.sizeOfEntry(mockTable);
        short pageCapacity = Page.getPageCapacity(TablePage.sizeOfEntry(mockTable));

        int expectedHeaderSize = Page.SIZE_OF_HEADER;
        int expectedEntriesSize = actualEntrySize * pageCapacity;

        assertEquals(expectedHeaderSize, page.sizeOfHeader());
        assertEquals(expectedEntriesSize, page.sizeOfEntries());

        // Assuming page.sizeInBytes() internally calls table.pageSizeInBytes() or calculates similarly.
        // If Table.pageSizeInBytes() is not public/protected, this specific assertion might need adjustment
        // by making that method accessible for testing or only testing what `Page` directly calculates.
        assertEquals(Page.pageSizeInBytes(TablePage.sizeOfEntry(mockTable)), page.sizeInBytes());
    }
    @AfterAll
    static void end(){
        File file = new File("./mockDB.mockTable.table");
        file.delete();
    }
}