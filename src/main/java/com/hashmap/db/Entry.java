package com.hashmap.db;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Entry {
    private byte[] ID;//Entryes primary key
    private ArrayList<Object> values; //rest of the rows values
    private int[] sizeOfElementsOfEntry;
    private int[] indexOfElemntsOfEntry;
    
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
    
    //Constructor
        public Entry(ArrayList<Object> newEntry){
            this.ID = ((String) newEntry.get(0)).getBytes(StandardCharsets.UTF_8);// fisrt element of a Row is must [Always] be the Primary Key and always converted to String on the background.
            this.sizeOfElementsOfEntry = this.getSizeOfElementsOfEntry(newEntry);
            this.indexOfElemntsOfEntry = this.getIndexOfElemntsOfEntry(this.sizeOfElementsOfEntry);
            newEntry.remove(0);
            newEntry.add(0, this.ID);
            this.values = newEntry;
        }

    public byte[] getID(){
        return this.ID;
    }

    public ArrayList<Object> getValues(){
        return this.values;
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

    //the following gives the size the entry would take when stored
    public int getEntrySizeInBytes(){
        ArrayList<Object> tmpEntry = new ArrayList<Object>(this.values);
        tmpEntry.remove(0);
        int sum = 0;
        for (Object element : tmpEntry) {
            if(element instanceof String){
                byte[] strBytes = ((String) element).getBytes(StandardCharsets.UTF_8);
                sum += strBytes.length + Short.BYTES;//Size of an element is saved in a (short) at the start of an element in the file so Short.BYTES accounts for the size that the size of the short that the size of the element is stored
            }else if(element instanceof Integer){
                sum += Integer.BYTES;
            }else if(element instanceof byte[]){
                byte[] byteArray = (byte[]) element;
                sum += byteArray.length + Short.BYTES;
            }else{
                throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        sum += this.getID().length;
        return sum;
    }
}
