package com.database.db.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIOThread;
import com.database.db.index.PrimaryKey;
//import com.database.db.index.SecondaryKey;
import com.database.db.page.PageCache;

public class Table<K extends Comparable<? super K>> {
    private String Database;
    private String tableName;
    private Schema tableSchema;
    private PageCache<K> cache;//Tables Cache.
    private PrimaryKey<K> primaryKey;
    //private ArrayList<SecondaryKey<?,?>> secondaryKeyIndexes;

    private short numOfColumns;
    private short maxEntriesPerPage;
    private int sizeOfEntry;
    private int primaryKeyColumnIndex;
    private static int SIZE_OF_HEADER = 2 * (Integer.BYTES) + Short.BYTES;
    private final int BLOCK_SIZE = 4096;
    public final int MAX_NUM_OF_PAGES_IN_CACHE = 10;
    private int numberOfPages;
    private FileIOThread fileIOThread;

    public Table(String databaseName, String tableName,Schema schema, FileIOThread fileIOThread) throws ExecutionException, InterruptedException, IOException {
        this.Database = databaseName;
        this.tableName = tableName;
        this.tableSchema = schema;
        this.cache = new PageCache<>(this);
        this.numOfColumns = (short) schema.getSizes().length;
        this.sizeOfEntry = this.setSizeOfEntry();
        this.primaryKeyColumnIndex = this.tableSchema.getPrimaryKeyIndex();
        this.maxEntriesPerPage = this.setPageMaxNumOfEntries();
        this.fileIOThread = fileIOThread;
        this.primaryKey = new PrimaryKey<K>(this.maxEntriesPerPage);
        this.primaryKey.initialize(this);
        this.numberOfPages = Math.ceilDiv(this.primaryKey.size(),this.maxEntriesPerPage);
    }
    private int setSizeOfEntry(){
        int result = 0;
        int[] columnSizes = this.tableSchema.getSizes();
        for (int i : columnSizes) {
            result += i;
        }
        return result;
    }

    public boolean isValidEntry(Entry<?> entry){
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.getEntry().size() != this.numOfColumns) return false;

        // Get the entry data and check each element's type and size.
        ArrayList<Object> entryData = entry.getEntry();
        Type[] expectedTypes = this.tableSchema.getTypes();
        int[] expectedSizes = this.tableSchema.getSizes();

        for (int i = 0; i < this.numOfColumns; i++) {
            Type expectedType = expectedTypes[i];
            int expectedSize = expectedSizes[i];
            Object value = entryData.get(i);
            // Validate type and size of each entry element.
            expectedType.validateValue(value, expectedSize);
        }
        // All checks passed, return true.
        return true;
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
    
    public PrimaryKey<K> getPrimaryKey(){
        return (PrimaryKey<K>) this.primaryKey;
    }public void setPrimaryKey(PrimaryKey<K> primaryKey){
        this.primaryKey = primaryKey;
    }
    public PageCache<?> cache(){
        return this.cache;
    }
    //Get Index and Table file paths for this Table. 
    public String getTablePath(){return "storage/"+this.getDatabaseName()+"."+this.getTableName()+".table";}
    public String getIndexPath(){return "storage/index/"+this.getDatabaseName()+"."+this.getTableName()+".index";}

    public int getNumOfColumns(){return this.numOfColumns;}
    public int getSizeOfEntry(){return this.sizeOfEntry;}
    public int getPrimaryKeyMaxSize(){return this.tableSchema.getSizes()[0];}
    public int getPrimaryKeyColumnIndex(){return this.primaryKeyColumnIndex;}
    public Type getPrimaryKeyType(){return this.getSchema().getTypes()[this.getPrimaryKeyColumnIndex()];}
    public FileIOThread getFileIOThread() {return this.fileIOThread;}
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
    }
    public short getEntriesPerPage(){return this.maxEntriesPerPage;}
    public int getPages(){return this.numberOfPages;}
    public void addOnePage(){this.numberOfPages++;}
    public void removeOnePage(){this.numberOfPages--;}
}
