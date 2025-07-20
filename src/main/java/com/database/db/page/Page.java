package com.database.db.page;

import java.io.IOException;

import com.database.db.table.Entry;
import com.database.db.table.Table;

public abstract class Page {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private Entry[] entries;
    private int maxSizeOfEntry;

    private int BLOCK_SIZE = 4096;
    private boolean dirty = false;

    public Page(int PageID, Table table) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new Entry[table.getPageCapacity()];
        this.maxSizeOfEntry = table.getSizeOfEntry();
    }

    // ==========ADDING_ENTRIES==========
    public void add(Entry entry){
        this.add(this.numOfEntries, entry);
    }
    public void add(int index, Entry entry) {
        if (entry == null) throw new IllegalArgumentException("Cannot add null Entry.");
        if (index < 0 || index >= this.entries.length) throw new IllegalArgumentException("Index out of bounds: " + index);
        if (this.numOfEntries == this.entries.length) throw new IllegalArgumentException("This page is full, max size: " + this.entries.length);
        if (this.entries[index] != null) throw new IllegalArgumentException("Entry already exists at index " + index+" can not add a new one");
        this.numOfEntries++;
        this.entries[index] = entry;
        this.spaceInUse += entry.sizeInBytes();
        this.dirty = true;
    }

    // ===========REMOVING_ENTRIES===============
    public <K extends Comparable<? super K>> Entry remove(K key, int columnIndex){
        int index = this.getIndex(key, columnIndex);
        return this.remove(index);
    }
    public Entry remove(int index) {
        if (index >= this.numOfEntries || index < 0)
            throw new IllegalArgumentException("Out of bounds Index you gave: " + index+" Maximum: "+this.numOfEntries);
        Entry result = this.entries[index];
        if(result == null)
            throw new IllegalArgumentException("Null Entry to remove for Index: "+index+" Maximum: "+this.numOfEntries);
        this.swapWithLast(index);
        this.entries[this.numOfEntries-1] = null;
        this.spaceInUse -= result.sizeInBytes();
        this.numOfEntries--;
        this.dirty = true;
        return result;
    }
    public Entry removeLast(){
        return this.remove(this.numOfEntries-1);
    }
    public void swapWithLast(int index){
        Entry result = this.entries[this.numOfEntries-1];
        this.entries[this.numOfEntries-1] = this.entries[index];
        this.entries[index] = result;
    }

    // ===========SEARCHING_ENTRIES===============
    public <K extends Comparable<? super K>> Entry get(K key, int columnIndex){
        int index = this.getIndex(key, columnIndex);
        if(index<0) throw new IllegalArgumentException("Entry with key :"+key+" can not be found.");
        return this.get(index);
    }
    public Entry get(int index) {
        if (index < 0 || this.entries.length == 0) throw new IndexOutOfBoundsException("invalid index You gave :" + index+" Size :"+this.entries.length);
        return this.entries[index];
    }
    public boolean contains(Entry entry){
        return this.indexOf(entry) != -1;
    }

    public Entry getLast(){
        return this.get(this.numOfEntries-1);
    }

    public Entry[] getAll() {return this.entries;}

    public int indexOf(Entry entry){
        int index = 0;
        for (Entry entryComp : this.entries) {
            if(entry.equals(entryComp)) return index;
        }
        return -1;
    }

    @SuppressWarnings("Unchecked")
    private <K extends Comparable<? super K>> int getIndex(K key, int columnIndex){
        for (int i = 0;i < this.numOfEntries;i++){
            Comparable<K> value = (Comparable<K>) this.entries[i].get(columnIndex);
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
                "\n\tSpace in Use :            [ " + this.spaceInUse + "/" + this.sizeOfEntries() + " ]";
    }

    public abstract byte[] toBytes() throws IOException;
    public abstract void fromBytes(byte[] bufferData) throws IOException;

    public int getPageID() {return this.pageID;}
    public void setPageID(int newPageID) {this.pageID = newPageID;}
    public int getSpaceInUse() {return this.spaceInUse;}

    public short size() {return this.numOfEntries;}

    public int sizeInBytes() {return (sizeOfEntries() + sizeOfHeader())+ (BLOCK_SIZE - ((sizeOfEntries() + sizeOfHeader()) % BLOCK_SIZE));}
    public int sizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    public int sizeOfEntries() {return (maxSizeOfEntry * this.entries.length);}

    public int getPagePos() {return this.pageID * this.sizeInBytes();}

    public boolean isDirty() {return dirty;}
    public void setDirty(boolean dirty) {this.dirty = dirty;}
}
