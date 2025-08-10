package com.database.db.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

import com.database.db.table.DataType;
import com.database.db.table.Schema;
import com.database.db.table.Table;


public class Entry {
    
    private Object[] entryData;
    private int[] sizeOfElementsOfEntry;
    private int[] indexOfElementsOfEntry;
    private BitSet nullsBitMap;
    private int numberOfNullableColumns;

    public Entry(){}

    //Constructor
    public Entry(Object[] data, int numOfNullColumns){
        this.entryData = data;
        this.numberOfNullableColumns = numOfNullColumns;
        this.nullsBitMap = new BitSet(this.numberOfNullableColumns);
    }

    public Entry setBitMap(boolean[] notNullables){
        int bitSetIndex = 0;
        for (int i = 0; i < entryData.length; i++) {
            if (!notNullables[i]) { // column is nullable
                if (entryData[i] == null) this.nullsBitMap.set(bitSetIndex,true);
                else this.nullsBitMap.set(bitSetIndex,false);
                bitSetIndex++;
            }
        }
        return this;
    }

    public int size(){return this.entryData.length;}

    public byte[] toBytes(Table table) {
        int bitmapSize = (table.getSchema().numNullables()+7)/8;
        byte[] bitMapBytes = new byte[bitmapSize];
        byte[] bitMapBytesRaw = this.nullsBitMap.toByteArray();
        System.arraycopy(bitMapBytesRaw, 0, bitMapBytes, 0, bitMapBytesRaw.length);
        ByteBuffer buffer = ByteBuffer.allocate(table.getSizeOfEntry());
        buffer.put(bitMapBytes);
        for (int i = 0;i<this.entryData.length;i++) {
            Object value = this.entryData[i];
            DataType elem = table.getSchema().getTypes()[i];
            buffer.put(elem.toBytes(value));
        }
        return buffer.array();
    }

    public static Entry fromBytes(ByteBuffer buffer,Table table){
        int startPos = buffer.position();
        int expectedSize = table.getSizeOfEntry();
        // optional: validate enough bytes remain
        if (buffer.remaining() < expectedSize) {
            throw new IllegalArgumentException(
                    "Not enough bytes in buffer for one entry: " + buffer.remaining() + " < " + expectedSize);
        }
        Schema schema = table.getSchema();
        Object[] entry = new Object[table.getSchema().getNumOfColumns()];
        DataType[] types = schema.getTypes();

        int numOfNullColumns = schema.numNullables();
        int bitmapSize = (numOfNullColumns + 7) / 8;
        byte[] nullBitmapBytes = new byte[bitmapSize];
        buffer.get(nullBitmapBytes); // advances position
        BitSet nullBitmap = BitSet.valueOf(nullBitmapBytes);

        boolean[] notNullables = schema.getNotNull();
        int nullableBitCount = 0;
        for (int i = 0; i < entry.length; i++) {
            if (!notNullables[i] && nullBitmap.get(nullableBitCount++)) {
                entry[i] = null;
                continue;
            }
            entry[i] = types[i].fromBytes(buffer);
        }
        int bytesRead = buffer.position() - startPos;
        if (bytesRead >= expectedSize) {
            throw new IllegalStateException("Entry deserialization consumed " + bytesRead +
                    " bytes, expected " + expectedSize);
        }
        return new Entry(entry, numOfNullColumns).setBitMap(schema.getNotNull());
    }

    public static Entry prepareEntry(String[] columnNames, Object[] entry, Table table) {
        entry = Entry.setEntry(columnNames, entry, table);
        Schema schema = table.getSchema();
        int primaryKeyIndex = schema.getPrimaryKeyIndex();
        boolean[] notNullable = schema.getNotNull();
        boolean[] AutoIncrementing = schema.getAutoIncrementIndex();
        boolean[] hasUniqueIndex = schema.getUniqueIndex();
        int numOfNullColumns = schema.getNumOfColumns();
        for (int i = 0;i<notNullable.length;i++) {
            boolean isPrimaryKey = (i == primaryKeyIndex);
            boolean isUnique = (hasUniqueIndex[i] || isPrimaryKey);
            if (notNullable[i] && !AutoIncrementing[i] && entry[i] == null)
                throw new IllegalArgumentException("Gave null value for NOT_NULL field: "+schema.getNames()[i]);
            if (AutoIncrementing[i] && entry[i] == null) 
                entry[i] = table.getAutoIncrementing(i).getNextKey();
            else if (AutoIncrementing[i] && entry[i] != null)
                if(table.getAutoIncrementing(i).getKey() < (long)entry[i]) table.getAutoIncrementing(i).setNextKey((long)entry[i]);
            else if (entry[i] == null) 
                entry[i] = schema.getDefaults()[i];
            if(isUnique && table.isKeyFound(entry[i], i))
                throw new IllegalArgumentException("Already Existing value for Primary Key column: "+schema.getNames()[i]);
        }
        return new Entry(entry, numOfNullColumns).setBitMap(notNullable);
    }
    private static Object[] setEntry(String[] columnNames, Object[] entry, Table table){
        ArrayList<String> names = new ArrayList<>(Arrays.asList(table.getSchema().getNames()));
        Object[] result = new Object[names.size()];
        for (int i = 0;i<columnNames.length;i++) {
            String name = columnNames[i];
            int columnIndex = names.indexOf(name);
            if(columnIndex<0) throw new IllegalArgumentException("Invalid column to insert: "+name);
            result[columnIndex] = entry[i];
        }
        return result;
    }

    public static boolean isValidEntry(Object[] entry, Schema schema) {
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.length != schema.getNumOfColumns()) return false;
        // Get the entry data and check each element's type and size.
        DataType[] expectedTypes = schema.getTypes();
        int[] expectedSizes = schema.getSizes();
        boolean[] isNotNullable = schema.getNotNull();
        for (int i = 0; i < expectedTypes.length; i++) {
            DataType expectedType = expectedTypes[i];
            int expectedSize = expectedSizes[i];
            if(!isNotNullable[i] && entry[i] != null) expectedType.validateValue(entry[i], expectedSize);
        }
        // All checks passed, return true.
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entry other = (Entry) o;
        return Objects.equals(this.entryData, other.entryData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryData);
    }

    public Object get(int index){return this.entryData[index];}
    public Object[] getEntry(){return this.entryData;}
    public int[] getElementSizes(){return this.sizeOfElementsOfEntry;}
    public int[] getElementIndexes(){return this.indexOfElementsOfEntry;}
    public int getNumOfElements(){return this.indexOfElementsOfEntry[this.indexOfElementsOfEntry.length - 1];}
}
