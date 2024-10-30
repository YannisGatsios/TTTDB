package com.hashmap.db;

import java.util.ArrayList;
import java.util.Arrays;

public class Block extends FileIO {

    private int blockID;
    private short numOfEtries;
    private int spaceInUse;
    private ArrayList<String> indexOfEntries = new ArrayList<String>();
    private ArrayList<ArrayList<Object>> entries;

    private short maxNumOfEtries;
    private int maxSizeOfEntry;
    private int maxSizeOfID;

    public Block(int BlockID, short maxNumOfEtries) {
        this.blockID = BlockID;
        this.numOfEtries = 0;
        this.spaceInUse = 0;
        this.maxNumOfEtries = maxNumOfEtries;
        this.indexOfEntries.add("0:0"); // this is only a temporary starting value it gets removed later when the first
                                        // entry gets added to this block.
        this.entries = new ArrayList<ArrayList<Object>>();
    }

    private int getIndexOfEntry(int numOfEntry){
        return Integer.parseInt(this.indexOfEntries.get(numOfEntry).split(":")[1].trim());
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

    public ArrayList<String> getIndexOfEntries() {
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
                "\n\tBlock ID : " + this.blockID +
                "\n\tNumber Of Entries : " + this.numOfEtries + 
                "\n\tSpace in Use : " + this.spaceInUse + 
                "\n\tIndex Of Rows : " + this.indexOfEntries + 
                "\n\tEntry data : " + this.entries;
    }

    // ==========ADDING_ENTRIES==========

    // checks if the ID exists within the block.
    private boolean isIDInBlock(byte[] ID) {
        if (this.entries.isEmpty())
            return false;
        for (ArrayList<Object> ArrayListEntry : this.entries) {
            if (ArrayListEntry.get(0) instanceof byte[]) {
                byte[] entryID = (byte[]) ArrayListEntry.get(0);
                if (Arrays.equals(entryID, ID)) { // Use Arrays.equals to compare contents
                    return true;
                }
            }
        }
        return false;
    }

    private int getNewIndex(int sizeOfNewEntry){
        int lastIndex = Integer.parseInt(this.indexOfEntries.get(this.indexOfEntries.size()-1).split(":")[1]);
        return lastIndex+sizeOfNewEntry;
    }

    public void AddEntry(Entry newEntry) throws IllegalArgumentException {
        if (this.numOfEtries == this.maxNumOfEtries || this.isIDInBlock(newEntry.getID())) {
            throw new IllegalArgumentException("New Entry ID Already Exists In Block.");
        }
        this.numOfEtries++;
        this.entries.add(newEntry.getValues());

        this.indexOfEntries.add(new String(newEntry.getID()) + ":"+ this.getNewIndex(newEntry.getEntrySizeInBytes()));
        this.spaceInUse += newEntry.getEntrySizeInBytes();

        if (this.indexOfEntries.get(0) == "0:0")
            this.indexOfEntries.remove(0);
    }

    // ===========REMOVING_ENTRIES===============

    private String removeDifference(String currentEntryIndex, int size) {
        int result = Integer.parseInt(currentEntryIndex.split(":")[1]);
        return currentEntryIndex.split(":")[0] + ":" + (result-size);
    }

    private int getEntrySize(int numOfEntryToRemove){
        int start;
        int end;
        if(numOfEntryToRemove == 0){
            start = 0;
            end = this.getIndexOfEntry(numOfEntryToRemove);
        }else{
            start = this.getIndexOfEntry(numOfEntryToRemove-1);
            end = this.getIndexOfEntry(numOfEntryToRemove);
        }
        return end-start;
    }

    private void removeIndexFromIndexOfEntries(int numOfEntryToRemove) {
        int sizeOfEntryToRemove = this.getEntrySize(numOfEntryToRemove);
        this.spaceInUse = this.spaceInUse - sizeOfEntryToRemove;
        if (numOfEntryToRemove + 1 == this.numOfEtries) {
            this.indexOfEntries.remove(numOfEntryToRemove);
        } else {
            for (int i = numOfEntryToRemove; i < this.numOfEtries - 1; i++) {
                this.indexOfEntries.set(i, this.removeDifference(this.indexOfEntries.get(i+1),sizeOfEntryToRemove));
            }
            this.indexOfEntries.remove(this.numOfEtries - 1);
        }
    }

    public void removeEntry(byte[] entryID) throws IllegalArgumentException {
        if (!isIDInBlock(entryID)) {
            throw new IllegalArgumentException("Entry ID to Delete Dose Not Exist On Block.");
        } else if (this.numOfEtries == 1) {
            this.indexOfEntries.remove(0);
            this.indexOfEntries.add("0:0");
            this.spaceInUse = 0;
            this.numOfEtries--;
            this.entries.remove(0);
        } else {
            for (int i = 0; i < this.numOfEtries; i++) {
                ArrayList<Object> entry = this.entries.get(i);
                if (entry.get(0) instanceof byte[] && (byte[]) entry.get(0) == entryID) {
                    removeIndexFromIndexOfEntries(i);
                    this.numOfEtries--;
                    this.entries.remove(entry);
                }
            }
        }
    }
}
