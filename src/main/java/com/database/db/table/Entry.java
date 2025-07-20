package com.database.db.table;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Objects;


public class Entry {
    
    private Object[] entryData;
    private int[] sizeOfElementsOfEntry;
    private int[] indexOfElementsOfEntry;
    private BitSet nullsBitMap;
    private int sizeInBytes;
    private int numberOfNullableColumns;

    public Entry(){}

    //Constructor
    public Entry(Object[] data, Table table){
        this.entryData = data;
        this.nullsBitMap = this.setBitMap(table);
        this.sizeOfElementsOfEntry = this.setSizeOfElementsOfEntry(data);
        this.indexOfElementsOfEntry = this.setIndexOfElementsOfEntry(this.sizeOfElementsOfEntry);
        this.sizeInBytes = this.setEntrySizeInBytes();
    }

    private BitSet setBitMap(Table table){
        boolean[] notNullables = table.getSchema().getNotNull();
        this.numberOfNullableColumns = table.getSchema().numNullables(); 
        BitSet bitMap = new BitSet(this.numberOfNullableColumns);
        int bitSetIndex = 0;
        for (int i = 0; i < entryData.length; i++) {
            if (!notNullables[i]) { // column is nullable
                if (entryData[i] == null) bitMap.set(bitSetIndex,true);
                else bitMap.set(bitSetIndex,false);
                bitSetIndex++;
            }
        }
        return bitMap;
    }
    
    //the following returns an [] of the size for each value of the entry.
    //the only supported types are Integer,String and for buffers byte[].
    //[example] row = ["hello",100000,byte[10]]
    //"hello" size = 5, 10000 size = 4(because size of an Integer in bytes is 4), byte[10] size = 10.
    //output = [5,6,10]
    private int[] setSizeOfElementsOfEntry(Object[] newEntry){
        int[] sizes = new int[newEntry.length];
        int ind = 0;
        for (Object value : newEntry) {
            if(value == null){
                sizes[ind] = 0;
                continue;
            }
            Type type = Type.detect(value);
            int size = type.getSize(); 
            if(size != -1){
                sizes[ind] = size;
            }else{
                if (type == Type.VARCHAR) {
                    // optionally use UTF-8 byte length if needed
                    sizes[ind] = ((String) value).getBytes(StandardCharsets.UTF_8).length;
                } else if (type == Type.BINARY) {
                    sizes[ind] = ((byte[]) value).length;
                }
            }
            ind++;
        }
        return sizes;
    }
    
    //the following gives the index of the end of each value the row has.
    //[example] size of first element of row is 5, for second is 6 and third is 10 ["hello",100000,byte[10]]
    //"hello" size = 5, 10000 size = 4(because size of an Integer in bytes is 4), byte[10] size = 10.
    //the output will be [5,5+4,5+4+10] = [5,9,19]
    //        5     9         19
    //        |     |         |
    //    hello1000000000000000
    private int[] setIndexOfElementsOfEntry(int[] sizeOfElementsOfEntry) {
        int[] indexes = new int[sizeOfElementsOfEntry.length];
        int ind = 0;
        int sum = 0;
        for (int i : sizeOfElementsOfEntry) {
            sum = sum+i;
            indexes[ind] = sum;
            ind++;
        }
        return indexes;
    }

    // the following gives the size the entry would take when stored
    private int setEntrySizeInBytes() {
        int sum = 0;
        for (int element : this.sizeOfElementsOfEntry) {
            sum += element;
        }
        int bitmapBytes = (this.numberOfNullableColumns + 7) / 8; 
        sum += bitmapBytes;
        return sum;
    }

    public int sizeInBytes(){return this.sizeInBytes;}

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
            Type elem = table.getSchema().getTypes()[i];
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
        Object[] entry = new Object[table.getNumOfColumns()];
        Type[] types = schema.getTypes();

        int bitmapSize = (schema.numNullables() + 7) / 8;

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

        return new Entry(entry, table);
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
