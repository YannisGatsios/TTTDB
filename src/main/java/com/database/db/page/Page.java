package com.database.db.page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

import com.database.db.table.Entry;
import com.database.db.table.Table;

public abstract class Page<K extends Comparable<? super K>> {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private ArrayList<Entry<K>> entries;
    private short maxNumOfEntries;
    private int maxSizeOfEntry;

    private int BLOCK_SIZE = 4096;

    public Page(int PageID, Table<K> table) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new ArrayList<>();
        this.maxNumOfEntries = table.getEntriesPerPage();
        this.maxSizeOfEntry = table.getSizeOfEntry();
    }

    // ==========ADDING_ENTRIES==========
    public void add(Entry<K> newEntry) throws IllegalArgumentException {
        if (this.numOfEntries == this.maxNumOfEntries) throw new IllegalArgumentException("this Page is full, current Max Size : " + this.maxNumOfEntries);
        int index = this.getIndex(newEntry.getID());
        if(index<0) index = -index-1;
        this.numOfEntries++;
        this.entries.add(index, newEntry);
        this.spaceInUse += newEntry.size();
    }

    // ===========REMOVING_ENTRIES===============
    public void remove(K key){
        int index = this.getIndex(key);
        if(index<0) throw new IllegalArgumentException("Entry with key :"+key+" can not be found.");
        this.remove(index);
    }
    public void remove(int index) {
        if (index > this.entries.size() - 1 || index < 0) throw new IllegalArgumentException("Invalid Number OF Entry to remove out of bounds you gave : " + index);
        this.spaceInUse -= this.get(index).size();
        this.entries.remove(index);
        this.numOfEntries--;
    }
    public void removeLast(){
        this.remove(this.numOfEntries-1);
    }

    // ===========SEARCHING_ENTRIES===============
    public Entry<K> get(K key){
        int index = this.getIndex(key);
        if(index<0) throw new IllegalArgumentException("Entry with key :"+key+" can not be found.");
        return this.get(index);
    }
    public Entry<K> get(int index) {
        if (index < 0 || this.entries.size() == 0) {
            throw new IndexOutOfBoundsException("invalid index You gave :" + index+" Size :"+this.entries.size());
        }
        return this.entries.get(index);
    }

    public Entry<K> getLast(){
        return this.get(this.numOfEntries-1);
    }

    private int getIndex(K key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        Entry<K> tmp = new Entry<K>();
        tmp.setID(key);
        return Collections.binarySearch(this.entries, tmp, (e1, e2) -> e1.getID().compareTo(e2.getID()));
    }

    // ===========PRINTING===============
    @Override
    public String toString() {
        return "\nPage Stats :" +
                "\n\tPage ID :                 " + this.pageID +
                "\n\tNumber Of Entries :        " + this.numOfEntries +
                "\n\tSize Of Page :           [" + this.sizeInBytes() + "]" +
                "\n\tSize Of Page Header :   [" + this.sizeOfHeader() + "]" +
                "\n\tSpace in Use :            [ " + this.spaceInUse + "/" + this.sizeOfEntries() + " ]" +
                "\n\tEntry data :               " + String.join(", ", this.getEntriesList());
    }private String[] getEntriesList() {
        String[] result = new String[this.numOfEntries];
        int ind = 0;
        for (Entry<K> entry : this.entries) {
            result[ind] = entry.getEntry()
                    .stream().map(Object::toString)
                    .collect(Collectors.joining(", "));
            ind++;
        }
        return result;
    }

    public abstract byte[] toBuffer() throws IOException;
    public abstract void bufferToPage(byte[] bufferData) throws IOException;

    public int getPageID() {return this.pageID;}
    public void setPageID(int newPageID) {this.pageID = newPageID;}

    public short size() {return this.numOfEntries;}
    public int getSpaceInUse() {return this.spaceInUse;}
    public ArrayList<Entry<K>> getAll() {return this.entries;}

    public int sizeInBytes() {return (sizeOfEntries() + sizeOfHeader())+ (BLOCK_SIZE - ((sizeOfEntries() + sizeOfHeader()) % BLOCK_SIZE));}
    public int sizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    public int sizeOfEntries() {return (maxSizeOfEntry * maxNumOfEntries);}

    public int getPagePos() {return this.pageID * this.sizeInBytes();}
}
