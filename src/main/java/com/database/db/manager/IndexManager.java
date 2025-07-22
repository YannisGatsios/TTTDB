package com.database.db.manager;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.database.db.index.BPlusTree;
import com.database.db.index.BTreeSerialization;
import com.database.db.index.Index;
import com.database.db.index.PrimaryKey;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;
import com.database.db.table.DataType;
import com.database.db.index.Unique;

public class IndexManager {

    private Table table;
    Schema schema;
    private List<BTreeSerialization<?>> indexes;

    public IndexManager(Table table) throws InterruptedException, ExecutionException, IOException {
        this.table = table;
        this.schema = table.getSchema();
        this.indexes = new ArrayList<>(schema.getNumOfColumns());
        // Checking if primary key is set and then setting it
        if(schema.hasPrimaryKey) {
            int pkIndex = schema.getPrimaryKeyIndex();
            this.indexes.add(pkIndex,this.newPrimaryKey(pkIndex));
        }
        if(this.schema.hasUnique){
            boolean[] unIndexes = schema.getUniqueIndex();
            for (int i = 0;i<unIndexes.length;i++) 
                if(unIndexes[i]) this.indexes.add(i,this.newUnique(i));
        }
        if(this.schema.hasIndex){
            boolean[] inIndexes = schema.getIndexIndex();
            for (int i = 0;i<inIndexes.length;i++) 
                if(inIndexes[i]) this.indexes.add(i,this.newIndex(i));
        }
    }

    private PrimaryKey<?> newPrimaryKey(int pkIndex) throws InterruptedException, ExecutionException{
        DataType pkType = schema.getTypes()[pkIndex];
        PrimaryKey<?> primaryKey = switch (pkType) {
            case INT -> new PrimaryKey<Integer>(table, pkIndex);
            case LONG -> new PrimaryKey<Long>(table, pkIndex);
            case FLOAT -> new PrimaryKey<Float>(table, pkIndex);
            case DOUBLE -> new PrimaryKey<Double>(table, pkIndex);
            case VARCHAR -> new PrimaryKey<String>(table, pkIndex);
            case DATE -> new PrimaryKey<Date>(table, pkIndex);
            case TIMESTAMP -> new PrimaryKey<Timestamp>(table, pkIndex);
            default -> throw new IllegalArgumentException("Unsupported primary key type: " + pkType);
        };
        return primaryKey;
    }

    private Unique<?> newUnique(int uqIndex) throws InterruptedException, ExecutionException{
        DataType type = schema.getTypes()[uqIndex];
        return switch (type) {
            case INT -> new Unique<Integer>(table, uqIndex);
            case LONG -> new Unique<Long>(table, uqIndex);
            case FLOAT -> new Unique<Float>(table, uqIndex);
            case DOUBLE -> new Unique<Double>(table, uqIndex);
            case VARCHAR -> new Unique<String>(table, uqIndex);
            case DATE -> new Unique<Date>(table, uqIndex);
            case TIMESTAMP -> new Unique<Timestamp>(table, uqIndex);
            default -> throw new IllegalArgumentException("Unsupported type: " + type.name());
        };
    }

    private Index<?> newIndex(int skIndex) throws InterruptedException, ExecutionException{
        DataType type = schema.getTypes()[skIndex];
        return switch (type) {
            case INT -> new Index<Integer>(table, skIndex);
            case LONG -> new Index<Long>(table, skIndex);
            case FLOAT -> new Index<Float>(table, skIndex);
            case DOUBLE -> new Index<Double>(table, skIndex);
            case VARCHAR -> new Index<String>(table, skIndex);
            case DATE -> new Index<Date>(table, skIndex);
            case TIMESTAMP -> new Index<Timestamp>(table, skIndex);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }
    public boolean isIndexed(int columnIndex){
        return this.indexes.get(columnIndex) != null;
    }

    public Object getMax(int columnIndex){
        return this.indexes.get(columnIndex).getMax();
    }
    
    public <K extends Comparable<? super K>> List<BlockPointer> findRangeIndex(K upper, K lower, int columnIndex){
        return this.rangeSearch(upper, lower, this.indexes.get(columnIndex));
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> List<BlockPointer> rangeSearch(K upper, K lower,BTreeSerialization<?> index){
        return ((BTreeSerialization<K>) index).rangeSearch(upper, lower);
    }
    public <K extends Comparable<? super K>> List<BlockPointer> findBlock(K key, int columnIndex){
        return this.search(key, this.indexes.get(columnIndex));
    }
    @SuppressWarnings("unchecked")
    private <K extends Comparable<? super K>> List<BlockPointer> search(K key, BTreeSerialization<?> index){
        return ((BTreeSerialization<K>)index).search(key);
    }
    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> boolean isKeyFound(K key, int columnIndex){
        return ((BTreeSerialization<K>)this.indexes.get(columnIndex)).isKey(key);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>> void insertIndex(Entry entry, BlockPointer blockPointer){
        for (BTreeSerialization<?> index : this.indexes) {
            int columnIndex = index.getColumnIndex();
            Object key = entry.get(columnIndex);
            DataType type = schema.getTypes()[columnIndex];
            Class<?> expectedClass = type.getJavaClass();
            if(key == null && !index.isNullable()) throw new IllegalArgumentException("Null key not allowed at column " + columnIndex);
            if(key != null && !expectedClass.isInstance(key)) 
                throw new IllegalArgumentException(
                "Invalid secondary key type at column " + columnIndex + 
                ": expected " + expectedClass.getName() + 
                ", but got " + key.getClass().getName());
            ((BPlusTree<K,BlockPointer>) index).insert((K) key, blockPointer);
        }
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>,V> void removeIndex(Entry entry, BlockPointer blockPointer){
        for (BTreeSerialization<?> index : this.indexes) {
            int columnIndex = index.getColumnIndex();
            Object key = entry.get(columnIndex);
            DataType type = schema.getTypes()[columnIndex];
            Class<?> expectedClass = type.getJavaClass();
            if(key == null && !index.isNullable()) throw new IllegalArgumentException("Null key not allowed at column " + columnIndex);
            if (key != null && !expectedClass.isInstance(key)) 
                throw new IllegalArgumentException(
                "Invalid secondary key type at column " + columnIndex + 
                ": expected " + expectedClass.getName() + 
                ", but got " + key.getClass().getName());
            ((BTreeSerialization<K>) index).remove((K) key, blockPointer);
        }
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<? super K>,V> void updateIndex(Entry entry, BlockPointer newBlockPointer, BlockPointer oldBlockPointer){
        for (BTreeSerialization<?> index : this.indexes) {
            int columnIndex = index.getColumnIndex();
            Object key = entry.get(columnIndex);
            DataType type = schema.getTypes()[columnIndex];
            Class<?> expectedClass = type.getJavaClass();
            if(key == null && !index.isNullable()) throw new IllegalArgumentException("Null key not allowed at column " + columnIndex);
            if (key != null && !expectedClass.isInstance(key)) 
                throw new IllegalArgumentException(
                "Invalid secondary key type at column " + columnIndex + 
                ": expected " + expectedClass.getName() + 
                ", but got " + key.getClass().getName());
            if(index.isUnique())((BTreeSerialization<K>) index).update((K) key, newBlockPointer);
            else ((BTreeSerialization<K>) index).update((K) key, newBlockPointer, oldBlockPointer);
        }
    }

    public void initPrimaryKey(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        //this.tableSchema.update TODO
        this.indexes.add(this.newPrimaryKey(columnIndex));
        ((PrimaryKey<?>)this.indexes.get(columnIndex)).initialize(table);
    }
    public void initIndex(int columnIndex) throws InterruptedException, ExecutionException, IOException{
        //this.tableSchema.update TODO
        this.indexes.add(this.newIndex(columnIndex));
        ((Index<?>)this.indexes.getLast()).initialize(table);
    }
}
