package com.database.db.page;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.database.db.table.Entry;
import com.database.db.table.Table;

public class Page<K extends Comparable<K>> extends PageManager<K> {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private ArrayList<Entry<K>> entries;
    private short maxNumOfEntries;
    private int maxSizeOfEntry;

    private int BLOCK_SIZE = 4096;

    public Page() {
    }

    public Page(int PageID, Table<K> table) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new ArrayList<>();
        this.maxNumOfEntries = table.getPageMaxNumOfEntries();
        this.maxSizeOfEntry = table.getSizeOfEntry();
    }

    // ==========ADDING_ENTRIES==========
    public void add(Entry<K> newEntry) throws IllegalArgumentException {
        if (this.numOfEntries == this.maxNumOfEntries) {
            throw new IllegalArgumentException("this Page is full, current Max Size : " + this.maxNumOfEntries);
        }
        this.numOfEntries++;
        this.entries.add(newEntry);
        this.spaceInUse += newEntry.size();
    }

    // ===========REMOVING_ENTRIES===============
    public void remove(int index) {
        if (index > this.entries.size() - 1 || index < 0) {
            throw new IllegalArgumentException(
                "Invalid Number OF Entry to remove from Page out of bounds you gave : " + index);
        }
        System.out.println(this.entries.get(index).getID()+"--- remove from Page "+this.pageID);
        this.spaceInUse -= this.getEntrySize(index);
        this.entries.remove(index);
        this.numOfEntries--;
    }

    // ===========SEARCHING_ENTRIES===============
    public Entry<K> get(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Can not get Entry from Page out of bounds Index for value : " + index);
        }
        return this.entries.get(index);
    }

    public int getIndex(K key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        int ind = 0;
        while (ind < this.entries.size()) {
            K entryKey = this.entries.get(ind).getID();
            if (key.equals(entryKey)) {
                return ind;
            }
            ind++;
        }
        return -1;
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

    private int getEntrySize(int numOfEntry) {
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

    public List<Entry<K>> getAll() {
        return this.entries;
    }

    public int sizeInBytes() {
        return (sizeOfEntries() + sizeOfHeader())
                + (BLOCK_SIZE - ((sizeOfEntries() + sizeOfHeader()) % BLOCK_SIZE));
    }

    public int sizeOfHeader() {
        return (2 * (Integer.BYTES)) + Short.BYTES;
    }

    public int sizeOfEntries() {
        return (maxSizeOfEntry * maxNumOfEntries);
    }

    public int getPagePos() {
        return this.pageID * this.sizeInBytes();
    }
}
