package com.database.db.table;

import java.util.ArrayList;

import com.database.db.FileIO;
import com.database.db.index.PrimaryKey;
import com.database.db.index.SecondaryKey;
import com.database.db.page.PageCache;

public class Table {
    private String Database;
    private String tableName;
    private Schema tableSchema;
    private PageCache<?> cache;//Tables Cache.
    private PrimaryKey<?> primaryKeyIndex;
    private ArrayList<SecondaryKey<?,?>> secondaryKeyIndexes;

    private short numOfColumns;
    private short maxEntriesPerPage;
    private int sizeOfEntry;
    private int primaryKeyColumnIndex;
    private static int SIZE_OF_HEADER = 2 * (Integer.BYTES) + Short.BYTES;
    private final int BLOCK_SIZE = 4096;
    public final int MAX_NUM_OF_PAGES_IN_CACHE = 10;

    public Table(String databaseName, String tableName,Schema schema){
        this.Database = databaseName;
        this.tableName = tableName;
        this.tableSchema = schema;
        this.cache = this.setCache(this.getPrimaryKeyType());
        
        this.numOfColumns = (short) schema.getColumnSizes().length;
        this.sizeOfEntry = this.setSizeOfEntry();
        this.primaryKeyColumnIndex = this.tableSchema.getPrimaryKeyIndex();
        this.maxEntriesPerPage = this.setPageMaxNumOfEntries();
        this.primaryKeyIndex = this.setTableIndex(this.getPrimaryKeyType());
    }
    private PageCache<?> setCache(String KeyType) {
        switch (KeyType) {
            case "Integer":
                return new PageCache<Integer>(this);
            case "String":
                return new PageCache<String>(this);
            default:
                throw new IllegalArgumentException("Invalid primary key type. (From setting PageCache in Table.)");
        }
    }private PrimaryKey<?> setTableIndex(String KeyType){
        FileIO fileIO = new FileIO();
        PrimaryKey<?> tree;
        switch (KeyType) {
            case "Integer":
                tree = new PrimaryKey<Integer>(this.maxEntriesPerPage)
                .bufferToTree(fileIO.readTree(this.getIndexPath()), this);
                return tree;
            case "String":
                tree = new PrimaryKey<String>(this.maxEntriesPerPage)
                .bufferToTree(fileIO.readTree(this.getIndexPath()), this);
                return tree;
            default:
                throw new IllegalArgumentException("Invalid primary key type. (From reading primary key Table Index on Table.)");
        }
    }
    private int setSizeOfEntry(){
        int sum = 0;
        String type[] = this.tableSchema.getColumnTypes();
        int size[] = this.tableSchema.getColumnSizes();
        for (int i = 0;i < this.numOfColumns; i++) {
            if(i == 0){
                sum += size[i];
            }else if(type[i].equals("String")){
                sum += size[i] + Short.BYTES;
            }else if(type[i].equals("Integer")){
                sum += Integer.BYTES;
            }else if(type[i].equals("Byte")){
                sum += size[i] + Short.BYTES;
            }else{
                throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        return sum;
    }

    public boolean isValidEntry(Entry<?> entry){
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.getEntry().size() != this.numOfColumns) {
            return false;
        }

        // Get the entry data and check each element's type and size.
        ArrayList<Object> entryData = entry.getEntry();
        String[] expectedTypes = this.tableSchema.getColumnTypes();
        int[] expectedSizes = this.tableSchema.getColumnSizes();

        for (int i = 0; i < this.numOfColumns; i++) {
            Object value = entryData.get(i);
            String expectedType = expectedTypes[i];
            int expectedSize = expectedSizes[i];

            // Validate type and size of each entry element.
            if (!isValidType(value, expectedType) && i != 0) {
                return false;
            }if (!isValidSize(value, expectedSize)) {
                return false;
            }
        }

        // All checks passed, return true.
        return true;
    }
    // Helper method to check if the type of the element matches the expected type.
    private boolean isValidType(Object value, String expectedType) {
        switch (expectedType) {
            case "String":
                return value instanceof String;
            case "Integer":
                return value instanceof Integer;
            case "Byte":
                return value instanceof byte[];
            default:
                return false; // Invalid type in schema.
        }
    }
    // Helper method to check if the size of the element is within the expected
    // size.
    private boolean isValidSize(Object value, int expectedSize) {
        if (value instanceof String) {
            return ((String) value).length() <= expectedSize;
        } else if (value instanceof Integer) {
            return Integer.BYTES == expectedSize; // Integer size is always 4 bytes.
        } else if (value instanceof byte[]) {
            return ((byte[]) value).length <= expectedSize;
        }
        return false;
    }

    public String getDatabaseName(){
        return this.Database;
    }
    public String getTableName(){
        return this.tableName;
    }
    public Schema getSchema(){
        return this.tableSchema;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<K>> PrimaryKey<K> getPrimaryKeyIndex(){
        return (PrimaryKey<K>) this.primaryKeyIndex;
    }public <K extends Comparable<K>> void setPrimaryKeyIndex(PrimaryKey<K> primaryKey){
        this.primaryKeyIndex = primaryKey;
    }
    public PageCache<?> cache(){
        return this.cache;
    }
    //Get Index and Table file paths for this Table. 
    public String getTablePath(){
        return "storage/"+this.getDatabaseName()+"."+this.getTableName()+".table";
    }public String getIndexPath(){
        return "storage/index/"+this.getDatabaseName()+"."+this.getTableName()+".index";
    }

    public int getNumOfColumns(){
        return this.numOfColumns;
    }
    public int getSizeOfEntry(){
        return this.sizeOfEntry;
    }
    public int getPrimaryKeyMaxSize(){
        return this.tableSchema.getColumnSizes()[0];
    }
    public int getPrimaryKeyColumnIndex(){
        return this.primaryKeyColumnIndex;
    }
    public String getPrimaryKeyType(){
        return this.getSchema().getColumnTypes()[this.getPrimaryKeyColumnIndex()];
    }

    public short setPageMaxNumOfEntries(){
        if(!((BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry >= 3)){
            return this.setMaxEntriesPerPage(2);
        }
        return (short) ((BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry);
    }private short setMaxEntriesPerPage(int numOfPages){
        if(!((numOfPages*(BLOCK_SIZE-SIZE_OF_HEADER))/this.sizeOfEntry >= 3)){
            return this.setMaxEntriesPerPage(numOfPages + 1);
        }
        return (short) (numOfPages * (BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry);
    }public short getPageMaxNumOfEntries(){
        return this.maxEntriesPerPage;
    }
}
