package com.hashmap.db;

import java.util.HashMap;
import java.util.ArrayList;

public class Block extends FileIO {

    private int blockID;
    private short numOfEtries;
    private int spaceInUse;
    private HashMap<byte[], Short> indexOfEntries;;
    private ArrayList<ArrayList<Object>> entries;

    private short maxNumOfEtries;
    private int maxSizeOfEntry;
    private int maxSizeOfID;

    public Block(int BlockID, short maxNumOfEtries) {
        this.blockID = BlockID;
        this.numOfEtries = 0;
        this.spaceInUse = 0;
        this.maxNumOfEtries = maxNumOfEtries;
        this.indexOfEntries = new HashMap<>();
        this.entries = new ArrayList<ArrayList<Object>>();
    }

    // ==========ADDING_ENTRIES==========
    public void addEntry(Entry newEntry) throws IllegalArgumentException {
        if (this.numOfEtries == this.maxNumOfEtries || indexOfEntries.containsKey(newEntry.getID())) {
            throw new IllegalArgumentException("New Entry ID Already Exists In Block.");
        }
        this.numOfEtries++;
        this.entries.add(newEntry.getEntry());

        int sizeOfNewEntrty = newEntry.getEntrySizeInBytes();
        this.spaceInUse += sizeOfNewEntrty;
        this.indexOfEntries.put(newEntry.getID(), (short)this.spaceInUse);
    }

    // ===========REMOVING_ENTRIES===============
    public void removeEntry(byte[] entryID) throws IllegalArgumentException {
        if (!indexOfEntries.containsKey(entryID)) {
            throw new IllegalArgumentException("Entry ID to Delete Dose Not Exist On Block.");
        }
        for (int i = 0; i < this.numOfEtries; i++) {
            if (this.getEntryID(i) == entryID) {
                this.removeEntry(i);
            }
        }
    }

    private void removeEntry(int numOfEntry) {
        short sizeOfEntry = this.getEntrySize(numOfEntry);
        this.spaceInUse -= sizeOfEntry;
        if (numOfEntry == this.numOfEtries - 1) {//Checks If It's The First Entry.
            this.indexOfEntries.remove(this.entries.get(numOfEntry).get(0));
        } else {
            this.indexOfEntries.remove(this.getEntryID(numOfEntry));
            for (int i = numOfEntry; i < this.numOfEtries - 1; i++) {
                this.indexOfEntries.put(this.getEntryID(i+1), (short) (this.indexOfEntries.get(this.getEntryID(i+1))-sizeOfEntry));
            }
        }
        this.numOfEtries--;
        this.entries.remove(this.getEntry(numOfEntry));
    }

    private short getEntrySize(int numOfEntryToRemove){
        short start;
        short end;
        if(numOfEntryToRemove == 0){
            start = 0;
            end = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove));
        }else{
            start = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove-1));
            end = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove));
        }
        return (short)(end-start);
    }
    
    private byte[] getEntryID(int numOfEntry){
        return (byte[]) this.getEntry(numOfEntry).get(0);
    }

    private ArrayList<Object> getEntry(int numOfEntry){
        return this.entries.get(numOfEntry);
    }

    public void setMaxSizeOfEntry(int maxSizeOfEntry) {
        this.maxSizeOfEntry = maxSizeOfEntry;
    }
    public void setMaxSizeOfID(int maxSizeOfID){
        this.maxSizeOfID = maxSizeOfID;
    }

    public int getBlockID() {
        return this.blockID;
    }

    public short getNumOfEtries() {
        return this.numOfEtries;
    }

    public int getSpaceInUse() {
        return this.spaceInUse;
    }

    public HashMap<byte[], Short> getIndexOfEntries() {
        return this.indexOfEntries;
    }

    public ArrayList<ArrayList<Object>> getEntries() {
        return this.entries;
    }

    public int getMazSizeOfID(){
        return this.maxSizeOfID;
    }

    public int getSizeOfBlock() {
        return this.sizeOfEntries() + this.sizeOfHeader();
    }

    public int sizeOfHeader() {
        return 3 * (Integer.BYTES) + Short.BYTES+ ((this.maxSizeOfID + Integer.toString(this.maxSizeOfEntry).length() + 1) * this.maxNumOfEtries);
    }

    public int sizeOfEntries() {
        return this.maxSizeOfEntry * this.maxNumOfEtries;
    }

    public String blockStats() {
        return "\nBlock Stats :" + 
                "\n\tBlock ID :                 " + this.blockID +
                "\n\tNumber Of Entries :        " + this.numOfEtries + 
                "\n\tSize Of Block :           [" + this.getSizeOfBlock() + "]" +
                "\n\tSize Of Blocks Header :   [" + this.sizeOfHeader() + "]" +
                "\n\tSpace in Use :            [ " + this.spaceInUse + "/" + this.sizeOfEntries() + " ]" +
                "\n\tIndex Of Rows :            " + this.indexOfEntries + 
                "\n\tEntry data :               " + this.entries;
    }

}
