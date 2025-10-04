package com.database.db.page;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

import com.database.db.index.BTreeInit.BlockPointer;
import com.database.db.table.DataType;
import com.database.db.table.SchemaInner;
import com.database.db.table.Table;

public class Entry {
    private Object[] values;
    private BitSet nullsBitMap;

    public Entry(){}

    //Constructor
    public Entry(Object[] data, int numOfNullColumns){
        this.values = data;
        this.nullsBitMap = new BitSet(numOfNullColumns);
    }

    public Entry setBitMap(boolean[] notNullables){
        int bitSetIndex = 0;
        for (int i = 0; i < values.length; i++) {
            if (!notNullables[i]) { // column is nullable
                this.nullsBitMap.set(bitSetIndex, values[i] == null);
                bitSetIndex++;
            }
        }
        return this;
    }

    public Entry deepCopy() {
        // clone the values array and the BitSet
        Object[] valuesCopy = Arrays.copyOf(this.values, this.values.length);
        BitSet bitmapCopy = (BitSet) this.nullsBitMap.clone();
        Entry copy = new Entry(valuesCopy, bitmapCopy.length());
        copy.nullsBitMap = bitmapCopy;
        return copy;
    }

    public int size(){return this.values.length;}

    public byte[] toBytes(Table table) {
        int bitmapSize = (table.getSchema().numNullables()+7)/8;
        byte[] bitMapBytes = new byte[bitmapSize];
        byte[] bitMapBytesRaw = this.nullsBitMap.toByteArray();
        System.arraycopy(bitMapBytesRaw, 0, bitMapBytes, 0, bitMapBytesRaw.length);
        ByteBuffer buffer = ByteBuffer.allocate(TablePage.sizeOfEntry(table));
        buffer.put(bitMapBytes);
        for (int i = 0;i<this.values.length;i++) {
            Object value = this.values[i];
            DataType elem = table.getSchema().getTypes()[i];
            buffer.put(elem.toBytes(value));
        }
        return buffer.array();
    }

    public static Entry fromBytes(ByteBuffer buffer,Table table){
        int startPos = buffer.position();
        int expectedSize = TablePage.sizeOfEntry(table);
        // optional: validate enough bytes remain
        if (buffer.remaining() < expectedSize)
            throw new IllegalArgumentException("Not enough bytes in buffer for one entry: "+buffer.remaining()+" < "+expectedSize);
        SchemaInner schema = table.getSchema();
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
        if (bytesRead > expectedSize)
            throw new IllegalStateException("Entry deserialization consumed "+bytesRead+" bytes, expected "+expectedSize);
        return new Entry(entry, numOfNullColumns).setBitMap(schema.getNotNull());
    }

    public byte[] toBytes(Table table, int columnIndex) {
        boolean  isNullable = !table.getSchema().getNotNull()[columnIndex];
        byte[] bitMapBytes = new byte[isNullable? 1:0];
        byte[] bitMapBytesRaw = this.nullsBitMap.toByteArray();
        int copyLen = Math.min(bitMapBytesRaw.length, bitMapBytes.length);
        System.arraycopy(bitMapBytesRaw, 0, bitMapBytes, 0, copyLen);
        ByteBuffer buffer = ByteBuffer.allocate(IndexPage.sizeOfEntry(table, columnIndex));
        buffer.put(bitMapBytes);
        buffer.put(((BlockPointer)this.values[0]).toBytes());
        DataType type = table.getSchema().getTypes()[columnIndex];
        buffer.put(type.toBytes(this.values[1]));
        return buffer.array();
    }

    public static Entry fromBytes(ByteBuffer buffer,Table table, int columnIndex){
        int startPos = buffer.position();
        int expectedSize = IndexPage.sizeOfEntry(table, columnIndex);
        // optional: validate enough bytes remain
        if (buffer.remaining() < expectedSize)
            throw new IllegalArgumentException("Not enough bytes in buffer for one entry: "+buffer.remaining()+" < "+expectedSize);
        SchemaInner schema = table.getSchema();
        Object[] values = new Object[2];
        DataType type = schema.getTypes()[columnIndex];

        boolean isNullable = !schema.getNotNull()[columnIndex];
        int bitmapSize = isNullable? 1:0;
        byte[] nullBitmapBytes = new byte[bitmapSize];
        buffer.get(nullBitmapBytes); // advances position
        BitSet nullBitmap = BitSet.valueOf(nullBitmapBytes);

        boolean isNotNullable = schema.getNotNull()[columnIndex];
        values[0] = BlockPointer.fromBytes(buffer);
        if (!isNotNullable && nullBitmap.get(0)) values[1] = null;
        else values[1] = type.fromBytes(buffer);
        
        int bytesRead = buffer.position() - startPos;
        if (bytesRead > expectedSize)
            throw new IllegalStateException("Entry deserialization consumed "+bytesRead +" bytes, expected "+expectedSize);
        int numOfNullColumns = !isNotNullable? 1 : 0;
        return new Entry(values, numOfNullColumns).setBitMap(new boolean[]{true,isNotNullable});
    }

    public static Entry prepareEntry(String[] columnNames, Object[] entry, Table table) {
        // Align input to table column order
        entry = Entry.setEntry(columnNames, entry, table);
        SchemaInner schema = table.getSchema();

        // Fill defaults for null values
        for (int i = 0; i < entry.length; i++) {
            if(schema.getAutoIncrementIndex()[i]){
                if (entry[i] == null) {
                    long nextVal = table.nextAutoIncrementValue(i);
                    entry[i] = nextVal;
                } else {
                    long provided = (long) entry[i];
                    long current = table.getAutoIncrementValue(i);
                    if (provided > current) {
                        table.setAutoIncrementValue(i, provided);
                    }
                }
            }
            // If entry is null and column was not explicitly set
            if (entry[i] == null && !isSetColumn(columnNames, schema, i)) {
                // Special handling for TIMESTAMP
                if (schema.getTypes()[i] == DataType.TIMESTAMP) {
                    entry[i] = java.time.LocalDateTime.now();
                } else {
                    entry[i] = schema.getDefaults()[i];
                }
            }
        }
        return new Entry(entry, schema.getNumOfColumns()).setBitMap(schema.getNotNull());
    }
    private static boolean isSetColumn(String[] columns, SchemaInner schema, int columnIndex){
        for (String string : columns) {
            if(schema.getColumnIndex(string) == columnIndex) return true;
        }
        return false;
    }
    private static Object[] setEntry(String[] columnNames, Object[] entry, Table table){
        Object[] result = new Object[table.getSchema().getNumOfColumns()];
        for (int i = 0;i<columnNames.length;i++) {
            String name = columnNames[i];
            int columnIndex = table.getSchema().getColumnIndex(name);
            if(columnIndex<0) throw new IllegalArgumentException("Invalid column to insert: "+name);
            result[columnIndex] = entry[i];
        }
        return result;
    }

    public static boolean isValidEntry(Object[] entry, SchemaInner schema) {
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
        return Arrays.equals(this.values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    public Object get(int index){return this.values[index];}
    public void set(int index, Object value){this.values[index] = value;}
    public Object[] getEntry(){return this.values;}
}
