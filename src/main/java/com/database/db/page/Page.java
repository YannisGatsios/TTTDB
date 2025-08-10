package com.database.db.page;

import java.io.IOException;

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
    public int add(Entry entry){
        return this.add(this.numOfEntries, entry);
    }
    public int add(int index, Entry entry) {
        if (entry == null) throw new IllegalArgumentException("Cannot add null Entry.");
        if (index < 0 || index >= this.entries.length) throw new IllegalArgumentException("Index out of bounds: " + index);
        if (this.entries[index] != null) throw new IllegalArgumentException("Entry already exists at index " + index+" can not add a new one");
        this.numOfEntries++;
        this.entries[index] = entry;
        this.spaceInUse += this.maxSizeOfEntry;
        this.dirty = true;
        return index;
    }

    // ===========REMOVING_ENTRIES===============
    public Entry remove(int index) {
        if (index >= this.numOfEntries || index < 0)
            throw new IllegalArgumentException("Out of bounds Index you gave: " + index+" Maximum: "+this.numOfEntries);
        Entry result = this.entries[index];
        if(result == null)
            throw new IllegalArgumentException("Null Entry to remove for Index: "+index+" Maximum: "+this.numOfEntries);
        this.swapWithLast(index);
        this.entries[this.numOfEntries-1] = null;
        this.spaceInUse -= this.maxSizeOfEntry;
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
    public Entry get(int index) {
        if (index < 0 || index >= this.numOfEntries) throw new IndexOutOfBoundsException("invalid index You gave :" + index+" Size :"+this.numOfEntries);
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
        for (int i = 0;i<this.numOfEntries;i++) {
            if(entry.equals(this.entries[i])) return i;
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

    public int getCapacity(){return this.entries.length;}
    public int getPagePos() {return this.pageID * this.sizeInBytes();}

    public boolean isDirty() {return dirty;}
    public void setDirty(boolean dirty) {this.dirty = dirty;}
}
