package com.database.db;

import java.util.ArrayList;

public class Entry<K> {
    
    private K ID;
    private ArrayList<Object> entryData;
    private int[] sizeOfElementsOfEntry;
    private int[] indexOfElementsOfEntry;
    private int sizeInBytes;

    //Constructor
    public Entry(ArrayList<Object> newEntry, int sizeOfID){
        this.entryData = newEntry;
        this.sizeOfElementsOfEntry = this.getSizeOfElementsOfEntry(newEntry);
        this.indexOfElementsOfEntry = this.getIndexOfElementsOfEntry(this.sizeOfElementsOfEntry);
        this.sizeInBytes = this.getEntrySizeInBytes();
    }
    
    //the following returns an [] of the size for each value of the entry.
    //the only supported types are Integer,String and for buffers byte[].
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
    
    //the following gives the index of the end of each value the row has.
    //[example] size of first element of row is 5, for second is 6 and third is 10 ["hello",100000,byte[10]]
    //"hello" size = 5, 10000 size = 4(because size of an Integer in bytes is 4), byte[10] size = 10.
    //the output will be [5,5+4,5+4+10] = [5,9,19]
    //        5     9         19
    //        |     |         |
    //    hello1000000000000000
    private int[] getIndexOfElementsOfEntry(int[] sizeOfElementsOfEntry) {
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
    private int getEntrySizeInBytes() {
        int sum = 0;
        for (int element : this.sizeOfElementsOfEntry) {
            sum += sum + element;
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    public void setID(int index){
        this.ID = (K)this.entryData.get(index);
    }public K getID(){
        return this.ID;
    }

    public int size(){
        return this.sizeInBytes;
    }

    public ArrayList<Object> getEntry(){
        return this.entryData;
    }

    public int[] getElementSizes(){
        return this.sizeOfElementsOfEntry;
    }

    public int[] getElementIndexes(){
        return this.indexOfElementsOfEntry;
    }

    public int getNumOfElements(){
        return this.indexOfElementsOfEntry[this.indexOfElementsOfEntry.length - 1];
    }
}
