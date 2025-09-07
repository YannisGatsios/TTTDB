package com.database.db.page;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Page {

    private int pageID;
    private short numOfEntries;
    private int spaceInUse;
    private final Entry[] entries;
    private final int sizeOfEntry;

    public static final int BLOCK_SIZE = 4096;
    public static final int SIZE_OF_HEADER = 2*Integer.BYTES + Short.BYTES;
    private boolean dirty = false;

    public Page(int PageID, int sizeOfEntry) {
        this.pageID = PageID;
        this.numOfEntries = 0;
        this.spaceInUse = 0;
        this.entries = new Entry[Page.getPageCapacity(sizeOfEntry)];
        this.sizeOfEntry = sizeOfEntry;
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
        this.spaceInUse += this.sizeOfEntry;
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
        this.spaceInUse -= this.sizeOfEntry;
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

    public static ByteBuffer headerToBytes(Page page){
        ByteBuffer buffer = ByteBuffer.allocate(page.sizeOfHeader());
        // Add primitive fields
        buffer.putInt(page.getPageID()); // Serialize pageID as 4 bytes (int)
        buffer.putShort(page.size()); // Serialize numOfEntries as 2 bytes (short)
        buffer.putInt(page.getSpaceInUse()); // Serialize spaceInUse as 4 bytes (int)
        buffer.flip();
        return buffer;
    }
    public record HeaderValues(int numOfEntries, int spaceInUse){}
    public static HeaderValues headerFromBytes(ByteBuffer buffer, Page page){
        int pageID = buffer.getInt();
        page.setPageID(pageID);
        short numOfEntries = buffer.getShort();
        int spaceInUse = buffer.getInt();
        return  new HeaderValues(numOfEntries, spaceInUse);
    }

    public static short getPageCapacity(int sizeOfEntry){
        if ((SIZE_OF_HEADER + sizeOfEntry) > BLOCK_SIZE) return 1;
        return (short) ((BLOCK_SIZE - SIZE_OF_HEADER) / sizeOfEntry);
    }
    public static int pageSizeOfEntries(int sizeOfEntry) {
        int capacity = Page.getPageCapacity(sizeOfEntry);
        return (sizeOfEntry * capacity);
    }
    public static int pageSizeInBytes(int sizeOfEntry) {
        int sizeOfEntries = Page.pageSizeOfEntries(sizeOfEntry);
        int total = sizeOfEntries + SIZE_OF_HEADER;
        return ((total + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
    }

    public abstract byte[] toBytes() throws IOException;
    public abstract void fromBytes(byte[] bufferData) throws IOException;

    public int getPageID() {return this.pageID;}
    public void setPageID(int newPageID) {this.pageID = newPageID;}
    public int getSpaceInUse() {return this.spaceInUse;}

    public short size() {return this.numOfEntries;}

    public int sizeInBytes() {return Page.pageSizeInBytes(sizeOfEntry);}
    public int sizeOfHeader() {return (2 * (Integer.BYTES)) + Short.BYTES;}
    public int sizeOfEntries() {return (sizeOfEntry * this.entries.length);}

    public int getCapacity(){return this.entries.length;}
    public int getPagePos() {return this.pageID * this.sizeInBytes();}

    public boolean isDirty() {return dirty;}
    public void setDirty(boolean dirty) {this.dirty = dirty;}
}
