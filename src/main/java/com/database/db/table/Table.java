package com.database.db.table;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIOThread;
import com.database.db.index.PrimaryKey;
import com.database.db.page.Cache;

public class Table {
    private String databaseName;
    private String tableName;
    private Schema tableSchema;
    private Cache cache;
    private TableIndexes indexes;

    private short entriesPerPage;
    private int sizeOfEntry;
    public static int SIZE_OF_HEADER = 2 * (Integer.BYTES) + Short.BYTES;
    private final int BLOCK_SIZE = 4096;
    public int CACHE_CAPACITY = 10;
    private int numberOfPages;
    private String path = "storage/";
    private FileIOThread fileIOThread;

    public Table(String databaseName, String tableName,Schema schema, FileIOThread fileIOThread,int CACHE_CAPACITY) throws ExecutionException, InterruptedException, IOException, Exception {
        this(databaseName,tableName,schema,fileIOThread);
        this.CACHE_CAPACITY = CACHE_CAPACITY;
    }

    public Table(String databaseName, String tableName,Schema schema, FileIOThread fileIOThread) throws ExecutionException, InterruptedException, IOException, Exception {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableSchema = schema;
        this.sizeOfEntry = this.setSizeOfEntry();
        this.entriesPerPage = this.setPageMaxNumOfEntries();
        this.fileIOThread = fileIOThread;
        this.cache = new Cache(this);
        this.indexes = this.initIndex();
        this.numberOfPages = this.setNumOfPages();
    }

    private int setNumOfPages(){
        File file = new File(this.getPath());
        long fileSize = file.length(); // size in bytes
        int pageSize = pageSizeInBytes(); // int

        // ceiling division for long / int:
        return (int) ((fileSize + pageSize - 1) / pageSize);
    }

    private int setSizeOfEntry(){
        int result = (this.tableSchema.numNullables()+7)/8;
        Type[] columnTypes = this.tableSchema.getTypes();
        int[] columnSizes = this.tableSchema.getSizes();
        for (int i = 0;i<columnTypes.length;i++) {
            int size = columnTypes[i].getSize();
            if(size == -1) size = columnSizes[i]+2;
            result += size;
        }
        return result;
    }

    private TableIndexes initIndex() throws InterruptedException, ExecutionException, IOException {
        TableIndexes indexes = new TableIndexes(this);
        return indexes;
    }

    public <K extends Comparable<? super K>> List<?> findRangeIndex(K upper, K lower, int columnIndex){
        return this.indexes.findRangeIndex(upper, lower, columnIndex);
    }
    public <K extends Comparable<? super K>> List<PrimaryKey.BlockPointer> findEntry(K key, int columnIndex){
        return this.indexes.findBlock(key, columnIndex);
    }
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        return this.indexes.isKeyFound(key, columnIndex);
    }

    public <K extends Comparable<? super K>> void insertIndex(Entry entry, PrimaryKey.BlockPointer blockPointer){
        this.indexes.insertIndex(entry, blockPointer);
    }
    public <K extends Comparable<? super K>> void removeIndex(Entry entry, PrimaryKey.BlockPointer blockPointer){
        this.indexes.removeIndex(entry, blockPointer);
    }

    public <K extends Comparable<? super K>> void updateIndex(Entry entry, PrimaryKey.BlockPointer newValue, PrimaryKey.BlockPointer odlValue){
        this.indexes.updateIndex(entry, newValue, odlValue);
    }
    public void initPrimaryKey(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        this.indexes.initPrimaryKey(columnIndex);
    }
    public void initSecondaryKey(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        this.indexes.initIndex(columnIndex);
    }

    public boolean isValidEntry(Entry entry) throws Exception{
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.size() != this.tableSchema.getNumOfColumns()) return false;

        // Get the entry data and check each element's type and size.
        Object[] entryData = entry.getEntry();
        Type[] expectedTypes = this.tableSchema.getTypes();
        int[] expectedSizes = this.tableSchema.getSizes();

        for (int i = 0; i < this.tableSchema.getNumOfColumns(); i++) {
            Object value = entryData[i];
            boolean isNullable = this.getSchema().getNotNull()[i]; 
            if(value == null && !isNullable){
                continue;
            }else if(value == null && isNullable){throw new Exception("Inserted null value to Non-Nullable field.");}// TODO handle exception better.
            Type expectedType = expectedTypes[i];
            int expectedSize = expectedSizes[i];
            expectedType.validateValue(value, expectedSize);
        }
        // All checks passed, return true.
        return true;
    }

    public String getDatabaseName(){
        return this.databaseName;
    }
    public String getName(){
        return this.tableName;
    }
    public Schema getSchema(){
        return this.tableSchema;
    }
    public Cache getCache(){return this.cache;}
    public PrimaryKey<?> getPrimaryKey(){
        return this.indexes.getPrimaryKey();
    }public void  setPrimaryKey(PrimaryKey<?> primaryKey){
        this.indexes.setPrimaryKey(primaryKey);
    }
    public Cache cache(){return this.cache;}

    //Get Index and Table file paths for this Table. 
    public String getPath(){return this.path+this.getDatabaseName()+"."+this.getName()+".table";}
    public String getIndexPath(int columnIndex){return this.path+this.getDatabaseName()+"."+this.getName()+"."+this.tableSchema.getNames()[columnIndex]+".index";}

    public int getNumOfColumns(){return this.tableSchema.getNumOfColumns();}
    public int getSizeOfEntry(){return this.sizeOfEntry;}
    public int getPrimaryKeyMaxSize(){return this.tableSchema.getSizes()[0];}
    public int getPrimaryKeyColumnIndex(){return this.tableSchema.getPrimaryKeyIndex();}
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
    public short getPageCapacity(){return this.entriesPerPage;}
    public int getPages(){return this.numberOfPages;}
    public void addOnePage(){this.numberOfPages++;}
    public void removeOnePage(){this.numberOfPages--;}
    private int pageSizeInBytes() {return (pageSizeOfEntries() + pageSizeOfHeader())+ (BLOCK_SIZE - ((pageSizeOfEntries() + pageSizeOfHeader()) % BLOCK_SIZE));}
    private int pageSizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    private int pageSizeOfEntries() {return (this.sizeOfEntry * this.entriesPerPage);}
}
