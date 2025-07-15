package com.database.db.page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import com.database.db.table.Entry;
import com.database.db.table.Table;

public abstract class Page {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private ArrayList<Entry> entries;
    private short maxNumOfEntries;
    private int maxSizeOfEntry;

    private int BLOCK_SIZE = 4096;
    private boolean dirty = false;

    public Page(int PageID, Table table) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new ArrayList<>();
        this.maxNumOfEntries = table.getEntriesPerPage();
        this.maxSizeOfEntry = table.getSizeOfEntry();
    }

    // ==========ADDING_ENTRIES==========
    public void add(Entry newEntry) throws IllegalArgumentException {
        if (this.numOfEntries == this.maxNumOfEntries) throw new IllegalArgumentException("this Page is full, current Max Size : " + this.maxNumOfEntries);
        this.numOfEntries++;
        this.entries.add(newEntry);
        this.spaceInUse += newEntry.size();
        this.dirty = true;
    }

    // ===========REMOVING_ENTRIES===============
    public <K extends Comparable<? super K>> Entry remove(K key, int columnIndex){
        int index = this.getIndex(key, columnIndex);
        return this.remove(index);
    }
    public Entry remove(int index) {
        if (index > this.entries.size() - 1 || index < 0) throw new IllegalArgumentException("Invalid Number OF Entry to remove out of bounds you gave : " + index);
        this.spaceInUse -= this.get(index).size();
        this.numOfEntries--;
        this.dirty = true;
        return this.entries.remove(index);
    }
    public Entry removeLast(){
        return this.remove(this.numOfEntries-1);
    }

    // ===========SEARCHING_ENTRIES===============
    public <K extends Comparable<? super K>> Entry get(K key, int columnIndex){
        int index = this.getIndex(key, columnIndex);
        if(index<0) throw new IllegalArgumentException("Entry with key :"+key+" can not be found.");
        return this.get(index);
    }
    public Entry get(int index) {
        if (index < 0 || this.entries.size() == 0) {
            throw new IndexOutOfBoundsException("invalid index You gave :" + index+" Size :"+this.entries.size());
        }
        return this.entries.get(index);
    }

    public Entry getLast(){
        return this.get(this.numOfEntries-1);
    }

    public int indexOf(Entry entry){
        return this.entries.indexOf(entry);
    }

    @SuppressWarnings("Unchecked")
    private <K extends Comparable<? super K>> int getIndex(K key, int columnIndex){
        for (int i = 0;i < this.numOfEntries;i++){
            Comparable<K> value = (Comparable<K>) this.entries.get(i).get(columnIndex);
            if(value.compareTo(key) == 0) return i;
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
        for (Entry entry : this.entries) {
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
    public int getSpaceInUse() {return this.spaceInUse;}
    public ArrayList<Entry> getAll() {return this.entries;}

    public short size() {return this.numOfEntries;}

    public int sizeInBytes() {return (sizeOfEntries() + sizeOfHeader())+ (BLOCK_SIZE - ((sizeOfEntries() + sizeOfHeader()) % BLOCK_SIZE));}
    public int sizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    public int sizeOfEntries() {return (maxSizeOfEntry * maxNumOfEntries);}

    public int getPagePos() {return this.pageID * this.sizeInBytes();}

    public boolean isDirty() {return dirty;}
    public void setDirty(boolean dirty) {this.dirty = dirty;}
}
