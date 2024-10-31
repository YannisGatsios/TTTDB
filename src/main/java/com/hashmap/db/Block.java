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
        this.indexOfEntries = new HashMap<>(); // this is only a temporary starting value it gets removed later when the first
                                        // entry gets added to this block.
        this.entries = new ArrayList<ArrayList<Object>>();
    }
    // ==========ADDING_ENTRIES==========

    public void AddEntry(Entry newEntry) throws IllegalArgumentException {
        if (this.numOfEtries == this.maxNumOfEtries || indexOfEntries.containsKey(newEntry.getID())) {
            throw new IllegalArgumentException("New Entry ID Already Exists In Block.");
        }
        byte[] newID = new byte[this.maxSizeOfID];
        System.arraycopy(newEntry.getID(), 0, newID, 0, newEntry.getID().length);

        ArrayList<Object> updatedNewEntry = newEntry.getValues();
        updatedNewEntry.remove(0);
        updatedNewEntry.add(0,newID);

        this.numOfEtries++;
        this.entries.add(updatedNewEntry);

        this.indexOfEntries.put(newID,(short)(this.spaceInUse+newEntry.getEntrySizeInBytes(newID.length)));
        this.spaceInUse += newEntry.getEntrySizeInBytes(newID.length);
    }

    // ===========REMOVING_ENTRIES===============

    private byte[] getEntryID(int entryNum){
        return (byte[]) this.entries.get(entryNum).get(0);
    }

    private short getEntrySize(int numOfEntryToRemove){
        short start;
        short end;
        if(numOfEntryToRemove == 0){
            start = 0;
            end = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove));//DEN IPARXEI getIndexOfEntry
        }else{
            start = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove-1));
            end = this.indexOfEntries.get(this.getEntryID(numOfEntryToRemove));
        }
        return (short)(end-start);
    }
    
    private void removeIndexFromIndexOfEntries(int numOfEntryToRemove) {
        short sizeOfEntryToRemove = this.getEntrySize(numOfEntryToRemove);
        this.spaceInUse = this.spaceInUse - sizeOfEntryToRemove;
        if (numOfEntryToRemove + 1 == this.numOfEtries) {
            this.indexOfEntries.remove(this.entries.get(numOfEntryToRemove).get(0));
        } else {
            this.indexOfEntries.remove(this.getEntryID(numOfEntryToRemove));
            for (int i = numOfEntryToRemove; i < this.numOfEtries - 1; i++) {
                this.indexOfEntries.put(this.getEntryID(i+1), (short) (this.indexOfEntries.get(this.getEntryID(i+1))-sizeOfEntryToRemove));
            }
        }
    }
    
    public void removeEntry(byte[] entryID) throws IllegalArgumentException {
        if (!indexOfEntries.containsKey(entryID)) {
            throw new IllegalArgumentException("Entry ID to Delete Dose Not Exist On Block.");
        } else if (this.numOfEtries == 1) {
            this.indexOfEntries.remove(entryID);
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
                "\n\tBlock ID : " + this.blockID +
                "\n\tNumber Of Entries : " + this.numOfEtries + 
                "\n\tSize Of Block : [" + this.getSizeOfBlock() + "]" +
                "\n\tSize Of Blocks Header : [" + this.sizeOfHeader() + "]" +
                "\n\tSpace in Use : [ " + this.spaceInUse + "/" + this.sizeOfEntries() + " ]" +
                "\n\tIndex Of Rows : " + this.indexOfEntries + 
                "\n\tEntry data : " + this.entries;
    }

}
