package com.database.db.page;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.database.db.Entry;
import com.database.db.Table;

public class Page extends PageManager {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private List<Entry> entries;
    private short maxNumOfEntries;
    private int maxSizeOfEntry;

    private static int BLOCK_SIZE = 4096;

    public Page(){}

    public Page(int PageID, Table table) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new ArrayList<>();
        this.maxNumOfEntries = table.getMaxEntriesPerPage();
        this.maxSizeOfEntry = table.getSizeOfEntry();
    }

    // ==========ADDING_ENTRIES==========
    public void add(Entry newEntry) throws IllegalArgumentException {
        if (this.numOfEntries == this.maxNumOfEntries) {
            throw new IllegalArgumentException("this Paage is full, current Max Size : " + this.maxNumOfEntries);
        }
        this.numOfEntries++;
        this.entries.add(newEntry);
        this.spaceInUse += newEntry.size();
    }

    // ===========REMOVING_ENTRIES===============
    public void remove(int index) {
        if(index > this.entries.size()-1 || index < 0){
            throw new IllegalArgumentException("Invalid Number OF Entry to remove out of bounds yu=ou gave :" + index);
        }
        this.spaceInUse -= this.getEntrySize(index);
        this.entries.remove(this.get(index));
        this.numOfEntries--;
    }

    //===========SEARCHING_ENTRIES===============
    public Entry get(int index){
        return this.entries.get(index);
    }

    public int getIndex(Object key){
        int ind = 0;
        for (Entry entry : this.entries) {
            if(entry.getID() == key){
                return ind;
            }
            ind++;
        }
        return -1;
    }

    private int getEntrySize(int numOfEntry){
        return this.entries.get(numOfEntry).size();
    }

    public int getPageID() {
        return this.pageID;
    }

    public short size() {
        return this.numOfEntries;
    }

    public int getSpaceInUse() {
        return this.spaceInUse;
    }

    public List<Entry> getEntries() {
        return this.entries;
    }

    public int sizeOfPage() {
        return (this.sizeOfEntries() + this.sizeOfHeader()) + ( BLOCK_SIZE - ((this.sizeOfEntries() + this.sizeOfHeader()) % BLOCK_SIZE) );
    }

    public int sizeOfHeader() {
        return 2 * (Integer.BYTES) + Short.BYTES;
    }

    public int sizeOfEntries() {
        return (this.maxSizeOfEntry * this.maxNumOfEntries);
    }

    public int getPagePos(){
        return this.pageID * this.sizeOfPage();
    }

    public String pageStats() {
        return "\nPage Stats :" + 
                "\n\tPage ID :                 " + this.pageID +
                "\n\tNumber Of Entries :        " + this.numOfEntries + 
                "\n\tSize Of Page :           [" + this.sizeOfPage() + "]" +
                "\n\tSize Of Page Header :   [" + this.sizeOfHeader() + "]" +
                "\n\tSpace in Use :            [ " + this.spaceInUse + "/" + this.sizeOfEntries() + " ]" +
                "\n\tEntry data :               " + String.join(", ", this.getEntriesList());
    }
    private String[] getEntriesList(){
        String[] result = new String[this.numOfEntries];
        int ind = 0;
        for (Entry entry : this.entries) {
            result[ind] = entry.getEntry()
                        .stream().map(Object::toString)
                        .collect(Collectors.joining(", "));
            ind++;
        }
        return result;
    }

}
