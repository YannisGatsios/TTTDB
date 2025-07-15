package com.database.db.table;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIOThread;
import com.database.db.index.Pair;
import com.database.db.index.PrimaryKey;
import com.database.db.index.SecondaryKey;
import com.database.db.page.Cache;

public class Table {
    private String databaseName;
    private String tableName;
    private Schema tableSchema;
    private Cache cache;
    private PrimaryKey<?> primaryKey = null;
    private ArrayList<SecondaryKey<?,?>> secondaryKeys;

    private short numOfColumns;
    private short entriesPerPage;
    private int sizeOfEntry;
    private int primaryKeyColumnIndex;
    public static int SIZE_OF_HEADER = 2 * (Integer.BYTES) + Short.BYTES;
    private final int BLOCK_SIZE = 4096;
    public final int CACHE_CAPACITY = 10;
    private int numberOfPages;
    private FileIOThread fileIOThread;

    public Table(String databaseName, String tableName,Schema schema, FileIOThread fileIOThread) throws ExecutionException, InterruptedException, IOException {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableSchema = schema;
        this.numOfColumns = (short) schema.getSizes().length;
        this.sizeOfEntry = this.setSizeOfEntry();
        this.primaryKeyColumnIndex = this.tableSchema.getPrimaryKeyIndex();
        this.entriesPerPage = this.setPageMaxNumOfEntries();
        this.fileIOThread = fileIOThread;
        this.cache = new Cache(this);
        this.secondaryKeys = new ArrayList<>();
        this.initIndex();
        this.numberOfPages = this.setNumOfPages();
    }

    private int setNumOfPages(){
        File file = new File("path/to/your/file");
        long fileSize = file.length(); // size in bytes
        int pageSize = pageSizeInBytes(); // int

        // ceiling division for long / int:
        return (int) ((fileSize + pageSize - 1) / pageSize);
    }

    private int setSizeOfEntry(){
        int result = 0;
        int[] columnSizes = this.tableSchema.getSizes();
        for (int i : columnSizes) {
            result += i;
        }
        return result;
    }

    private void initIndex() throws InterruptedException, ExecutionException, IOException {
        // Checking if primary key is set and then setting it
        int pkIndex = this.tableSchema.getPrimaryKeyIndex(); 
        if(pkIndex != -1) this.primaryKey = this.newPrimaryKey(pkIndex);
        // Checking if secondary key is set and then setting it
        int[] skIndexes = this.tableSchema.getSecondaryKeyIndex();
        if(skIndexes.length != 0){
            for (int i : skIndexes) this.secondaryKeys.add(this.newSecondaryKey(i));
        }
    }

    private PrimaryKey<?> newPrimaryKey(int pkIndex) throws InterruptedException, ExecutionException{
        Type pkType = this.tableSchema.getTypes()[pkIndex];
        PrimaryKey<?> primaryKey = switch (pkType) {
            case INT -> new PrimaryKey<Integer>(this);
            case LONG -> new PrimaryKey<Long>(this);
            case FLOAT -> new PrimaryKey<Float>(this);
            case DOUBLE -> new PrimaryKey<Double>(this);
            case STRING -> new PrimaryKey<String>(this);
            case DATE -> new PrimaryKey<Date>(this);
            case TIMESTAMP -> new PrimaryKey<Timestamp>(this);
            default -> throw new IllegalArgumentException("Unsupported primary key type: " + pkType);
        };
        return primaryKey;
    }

    private SecondaryKey<?, ?> newSecondaryKey(int skIndex) throws InterruptedException, ExecutionException{
        Type type = this.tableSchema.getTypes()[skIndex];
        return switch (type) {
            case INT -> new SecondaryKey<Integer, Object>(this, skIndex);
            case LONG -> new SecondaryKey<Long, Object>(this, skIndex);
            case FLOAT -> new SecondaryKey<Float, Object>(this, skIndex);
            case DOUBLE -> new SecondaryKey<Double, Object>(this, skIndex);
            case STRING -> new SecondaryKey<String, Object>(this, skIndex);
            case DATE -> new SecondaryKey<Date, Object>(this, skIndex);
            case TIMESTAMP -> new SecondaryKey<Timestamp, Object>(this, skIndex);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> List<?> findRangeIndex(K upper, K lower, int columnIndex){
        if(this.primaryKey != null && this.primaryKeyColumnIndex == columnIndex){
            PrimaryKey<K> pkIndex = (PrimaryKey<K>) this.primaryKey;
            List<Pair<K, Integer>> list = pkIndex.rangeSearch(upper, lower);
            return list;
        }
        for (SecondaryKey<?,?> secondaryKey : this.secondaryKeys) {
            if(secondaryKey.getColumnIndex() == columnIndex){
                SecondaryKey<K, Object> skIndex = (SecondaryKey<K, Object>) secondaryKey;
                List<Pair<K, Object>> list = skIndex.rangeSearch(upper, lower);
                List<Pair<K, Object>> result = new ArrayList<>();
                for (Pair<K, Object> pair : list) {
                    result.add(pair);
                    HashSet<Pair<K, Object>> duplicates = pair.getDuplicates();
                    if (duplicates != null) result.addAll(duplicates);
                }
                return result;
            }
        }
        return null;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> Integer findBlock(K key, int columnIndex){
        if(this.primaryKey != null && this.primaryKeyColumnIndex == columnIndex){
            PrimaryKey<K> pkIndex = (PrimaryKey<K>) this.primaryKey;
            Pair<K,Integer> pair = pkIndex.search(key);
            if (pair != null)return pair.value;
            else return null;
        }
        for (SecondaryKey<?,?> secondaryKey : secondaryKeys) {
            if (secondaryKey.getColumnIndex() == columnIndex){
                SecondaryKey<K, Object> skIndex = (SecondaryKey<K, Object>) this.secondaryKeys.get(0);
                Pair<K,Object> pair = skIndex.search(key);
                if (pair != null)return (Integer)pair.value;
            }
        }
        return null;
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        if(this.primaryKey != null && this.primaryKeyColumnIndex == columnIndex) return ((PrimaryKey<K>)this.primaryKey).isKey(key);
        for (SecondaryKey<?,?> secondaryKey : this.secondaryKeys) {
            if (secondaryKey.getColumnIndex() == columnIndex) return ((SecondaryKey<K,Object>)secondaryKey).isKey(key);
        }
        return false;
    }

    public <K extends Comparable<? super K>> void insertIndex(Entry entry, int BlockID){
        Object key = null;
        if (this.primaryKey != null) {
            key = entry.get(this.primaryKeyColumnIndex);
            this.insertToPrimaryKey(key, BlockID);
        }
        for (SecondaryKey<?, ?> secondaryKey : secondaryKeys) {
            int skIndex = secondaryKey.getColumnIndex();
            Object sKey = entry.get(skIndex);
            Type skType = tableSchema.getTypes()[skIndex];
            Class<?> expectedClass = skType.getJavaClass();
            if (!expectedClass.isInstance(sKey)) throw new IllegalArgumentException("invalid secondaryKey key type.");
            if(this.primaryKey == null)this.insertToSecondaryKey(secondaryKey, sKey, BlockID);
            else if(key != null) this.insertToSecondaryKey(secondaryKey, sKey, key);
        }
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> void insertToPrimaryKey(Object key, int blockID) {
        Type pkType = tableSchema.getTypes()[primaryKeyColumnIndex];
        Class<?> expectedClass = pkType.getJavaClass();
        if (!expectedClass.isInstance(key)) throw new IllegalArgumentException("invalid primaryKey key type.");
        ((PrimaryKey<K>) this.primaryKey).insert((K) key, blockID);
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>,V> void insertToSecondaryKey(SecondaryKey<K,V> secondaryKey, Object key, Object value) {
        ((SecondaryKey<K,V>) secondaryKey).insert((K) key, (V) value);
    }
    public <K extends Comparable<? super K>> void removeIndex(Entry entry, int BlockID){
        Object key = null;
        if(this.primaryKey != null){
            key = entry.get(this.primaryKeyColumnIndex);
            this.removeFromPrimaryKey(key, BlockID);
        }
        for (SecondaryKey<?, ?> secondaryKey : secondaryKeys) {
            int skIndex = secondaryKey.getColumnIndex();
            Object sKey = entry.get(skIndex);
            Type skType = tableSchema.getTypes()[skIndex];
            Class<?> expectedClass = skType.getJavaClass();
            if (!expectedClass.isInstance(sKey)) throw new IllegalArgumentException("invalid secondaryKey key type.");
            if(this.primaryKey == null)this.removeFromSecondaryKey(secondaryKey, sKey, BlockID);
            else if(key != null) this.removeFromSecondaryKey(secondaryKey, sKey, key);
        }
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> void removeFromPrimaryKey(Object key, int BlockID){
        Type pkType = tableSchema.getTypes()[this.primaryKeyColumnIndex];
        Class<?> expectedClass = pkType.getJavaClass();
        if (!expectedClass.isInstance(key)) throw new IllegalArgumentException("invalid primaryKey key type.");
        ((PrimaryKey<K>) this.primaryKey).remove((K) key, BlockID);
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>,V> void removeFromSecondaryKey(SecondaryKey<K,V> secondaryKey, Object key, Object value) {
        ((SecondaryKey<K,V>) secondaryKey).remove((K) key, (V) value);
    }

    public <K extends Comparable<? super K>> void updateIndex(Entry entry, int newBlockID, int oldBlockID){
        Object key = null;
        if(this.primaryKey != null){
            key = entry.get(this.primaryKeyColumnIndex);
            this.updateFromPrimaryKey(key, newBlockID);
        }
        for (SecondaryKey<?, ?> secondaryKey : secondaryKeys) {
            if(this.primaryKey == null){
                int skIndex = secondaryKey.getColumnIndex();
                Object sKey = entry.get(skIndex);
                Type skType = tableSchema.getTypes()[skIndex];
                Class<?> expectedClass = skType.getJavaClass();
                if (!expectedClass.isInstance(sKey)) throw new IllegalArgumentException("invalid secondaryKey key type.");
                this.updateFromSecondaryKey(secondaryKey, sKey, newBlockID, oldBlockID);
            }
        }
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> void updateFromPrimaryKey(Object key, int BlockID){
        Type pkType = tableSchema.getTypes()[this.primaryKeyColumnIndex];
        Class<?> expectedClass = pkType.getJavaClass();
        if (!expectedClass.isInstance(key)) throw new IllegalArgumentException("invalid primaryKey key type.");
        ((PrimaryKey<K>) this.primaryKey).update((K) key, BlockID);
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>,V> void updateFromSecondaryKey(SecondaryKey<K,V> secondaryKey, Object key, Object value, Object oldValue) {
        ((SecondaryKey<K,V>) secondaryKey).update((K) key, (V) value, (V)oldValue);
    }

    public void initPrimaryKey(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        this.primaryKeyColumnIndex = columnIndex;
        //this.tableSchema.update TODO
        this.primaryKey = this.newPrimaryKey(columnIndex);
        this.primaryKey.initialize(this);
    }
    public void initSecondaryKey(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        this.primaryKeyColumnIndex = columnIndex;
        //this.tableSchema.update TODO
        this.secondaryKeys.add(this.newSecondaryKey(columnIndex));
        this.secondaryKeys.getLast().initialize(this);
    }

    public boolean isValidEntry(Entry entry) throws Exception{
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.getEntry().size() != this.numOfColumns) return false;

        // Get the entry data and check each element's type and size.
        ArrayList<Object> entryData = entry.getEntry();
        Type[] expectedTypes = this.tableSchema.getTypes();
        int[] expectedSizes = this.tableSchema.getSizes();

        for (int i = 0; i < this.numOfColumns; i++) {
            Object value = entryData.get(i);
            boolean isNullable = this.getSchema().getNullable()[i]; 
            if(value == null && isNullable){
                continue;
            }else if(value == null && !isNullable){throw new Exception("Inserted null value to Non-Nullable field.");}// TODO handle exception better.
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
    public <K extends Comparable<? super K>> PrimaryKey<K> getPrimaryKey(){
        return (PrimaryKey<K>) this.primaryKey;
    }public <K extends Comparable<? super K>> void  setPrimaryKey(PrimaryKey<K> primaryKey){
        this.primaryKey = primaryKey;
    }
    public Cache cache(){return this.cache;}

    //Get Index and Table file paths for this Table. 
    public String getPath(){return "storage/"+this.getDatabaseName()+"."+this.getName()+".table";}
    public String getPKPath(){return "storage/index/"+this.getDatabaseName()+"."+this.getName()+".index";}
    public String getSKPath(int columnIndex){return "storage/index/"+this.getDatabaseName()+"."+this.getName()+"."+this.tableSchema.getNames()[columnIndex]+".index";}

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
    public short getEntriesPerPage(){return this.entriesPerPage;}
    public int getPages(){return this.numberOfPages;}
    public void addOnePage(){this.numberOfPages++;}
    public void removeOnePage(){this.numberOfPages--;}
    private int pageSizeInBytes() {return (pageSizeOfEntries() + pageSizeOfHeader())+ (BLOCK_SIZE - ((pageSizeOfEntries() + pageSizeOfHeader()) % BLOCK_SIZE));}
    private int pageSizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    private int pageSizeOfEntries() {return (this.sizeOfEntry * this.entriesPerPage);}
}
