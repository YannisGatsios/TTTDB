package com.database.db;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Entry {
    private byte[] ID;
    private ArrayList<Object> entryData;
    private int[] sizeOfElementsOfEntry;
    private int[] indexOfElemntsOfEntry;

    //Constructor
    public Entry(ArrayList<Object> newEntry, int sizeOfID){
        // fisrt element of a Row is must Always be the Primary Key And is allways converted to byte[].
        this.ID = this.refactorID(newEntry, sizeOfID);
        this.sizeOfElementsOfEntry = this.getSizeOfElementsOfEntry(newEntry);
        this.indexOfElemntsOfEntry = this.getIndexOfElemntsOfEntry(this.sizeOfElementsOfEntry);
        newEntry.remove(0);
        newEntry.add(0, this.ID);
        this.entryData = newEntry;
    }

    private byte[] refactorID(ArrayList<Object> newEntry, int sizeOfID){
        switch (newEntry.get(0).getClass().getSimpleName()) {
            case "Integer":
                ByteBuffer buffer = ByteBuffer.allocate(4); // Allocate 4 bytes
                buffer.putInt((int) newEntry.get(0));
                return buffer.array();
        
            case "String":
                byte[] oldIDString = ((String) newEntry.get(0)).getBytes(StandardCharsets.UTF_8);
                byte[] newIDString = new byte[sizeOfID];
                System.arraycopy(oldIDString, 0, newIDString, 0, oldIDString.length);
                return newIDString;
        
            case "byte[]":
                byte[] oldIDByteArray = (byte[]) newEntry.get(0);
                byte[] newIDByteArray = new byte[sizeOfID];
                System.arraycopy(oldIDByteArray, 0, newIDByteArray, 0, oldIDByteArray.length);
                return newIDByteArray;
        
            default:
                throw new IllegalArgumentException("Invalid Type Of ID (primary key).");
        }
    }
    
    //the following returns an [] of the size for eatch value of the erntry.
    //the only suported types are Integer,String and for buffers byte[].
    //[example] row = ["hello",100000,byte[10]]
    //"hello" size = 5, 10000 size = 4(because size of an Integer in bytes is 4), byte[10] size = 10.
    //output = [5,6,10]
    private int[] getSizeOfElementsOfEntry(ArrayList<Object> newEntry){
        int[] sizes = new int[newEntry.size()];
        int ind = 0;
        for (Object value : newEntry) {
            if(value instanceof Integer){
                sizes[ind] = Integer.BYTES;
            }else if(value instanceof String){
                sizes[ind] = ((String)value).length();
            }else if(value instanceof byte[]){
                sizes[ind] = ((byte[])value).length;
            }else{
                throw new IllegalArgumentException("Wrong type for Entry.");
            }
            ind++;
        }
        return sizes;
    }
    
    //the following gives the index of the end of eatch value the row hase.
    //[example] size of fisrt element of row is 5, forr second is 6 and third is 10 ["hello",100000,byte[10]]
    //"hello" size = 5, 10000 size = 4(because size of an Integer in bytes is 4), byte[10] size = 10.
    //the output will be [5,5+4,5+4+10] = [5,9,19]
    //        5     9         19
    //        |     |         |
    //    hello1000000000000000
    private int[] getIndexOfElemntsOfEntry(int[] sizeOfElementsOfEntry) {
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
    public int getEntrySizeInBytes() {
        ArrayList<Object> tmpEntry = new ArrayList<Object>(this.entryData);
        tmpEntry.remove(0);
        int sum = this.ID.length;
        for (Object element : tmpEntry) {
            switch (element.getClass().getSimpleName()) {
                case "String":
                    byte[] strBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
                    sum += strBytes.length + Short.BYTES; // String length + 2 bytes for length prefix
                    break;
                case "Integer":
                    sum += Integer.BYTES; // Add 4 bytes for an integer
                    break;
                case "byte[]":
                    byte[] byteArray = (byte[]) element;
                    sum += byteArray.length + Short.BYTES; // Byte array length + 2 bytes for length prefix
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        return sum;
    }

    public byte[] getID(){
        return this.ID;
    }

    public ArrayList<Object> getEntry(){
        return this.entryData;
    }

    public int[] getElementSizes(){
        return this.sizeOfElementsOfEntry;
    }

    public int[] getElementIndexes(){
        return this.indexOfElemntsOfEntry;
    }

    public int getNumOfElements(){
        return this.indexOfElemntsOfEntry[this.indexOfElemntsOfEntry.length - 1];
    }
}
