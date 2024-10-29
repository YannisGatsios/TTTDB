package com.hashmap.db;

import java.io.IOException;
import java.util.ArrayList;

public class Block extends FileIO {

    private int blockID;
    private short numOfEtries;
    private int spaceInUse;
    private short numOfColumns;
    private short maxNumOfEtries;
    private byte sizeOfIndexPerElement;
    private int maxSizeOfEntry;
    private ArrayList<String> indexOfEntries = new ArrayList<String>();

    private ArrayList<ArrayList<Object>> entries;

    public Block(int BlockID, short maxNumOfEtries) {
        this.blockID = BlockID;
        this.numOfEtries = 0;
        this.spaceInUse = 0;
        this.maxNumOfEtries = maxNumOfEtries;
        this.indexOfEntries.add("0:0"); // this is only a temporary starting value it gets removed later when the first
                                        // entry gets added to this block.
        this.entries = new ArrayList<ArrayList<Object>>();
    }

    public void setNumOfEntries(short numOfEtries) {
        this.numOfEtries = numOfEtries;
    }

    public void setSpaceInUse(int spaceInUse) {
        this.spaceInUse = spaceInUse;
    }

    public void setIndexOfEntries(ArrayList<String> indexOfEntries){
        this.indexOfEntries = indexOfEntries;
    }

    public void setSizeOfIndexPerElement(byte sizeOfIndexPerElement) {
        this.sizeOfIndexPerElement = sizeOfIndexPerElement;
    }

    public void setNumOfCumlumns(short numOfColumns) {
        this.numOfColumns = numOfColumns;
    }

    public void setMaxSizeOfEntry(int maxSizeOfEntry) {
        this.maxSizeOfEntry = maxSizeOfEntry;
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

    public int getSizeOfBlock() {
        return (this.maxSizeOfEntry * this.maxNumOfEtries) + 3 * (Integer.BYTES) + Short.BYTES
                + ((this.sizeOfIndexPerElement * this.numOfColumns) * this.maxNumOfEtries);
    }

    public int sizeOfHeader() {
        return 3 * (Integer.BYTES) + Short.BYTES
                + ((this.sizeOfIndexPerElement * this.numOfColumns) * this.maxNumOfEtries);
    }

    public int sizeOfEntries() {
        return this.maxSizeOfEntry * this.maxNumOfEtries;
    }

    public String blockStats() {
        return "Block Stats:" + "\n\tSpace in Use : " + this.spaceInUse + "\n\tIndex Of Rows : " + this.indexOfEntries
                + "\n\tNumOfEntries : " + this.numOfEtries + "\n\tErtry data : " + this.entries;
    }

    // ==========ADDING_ENTRIES==========

    private String addOldIndexToNewEntryIndexes(int[] indexesOfEntry, int lastIndex) {
        String result = "";
        for (int i : indexesOfEntry) {
            result = result + "," + String.valueOf(lastIndex + i);
        }
        return result.substring(1);
    }

    // checks if the ID exists within the block.
    private boolean IsIDInBLock(byte[] ID) {
        if (this.entries.isEmpty())
            return false;
        for (ArrayList<Object> ArrayListEntry : this.entries) {
            System.out.println(ArrayListEntry.get(0));
            if (ArrayListEntry.get(0) instanceof byte[] && (byte[]) ArrayListEntry.get(0) == ID) {
                return true;
            }
        }
        return false;
    }

    public void AddEntry(Entry newEntry)
            throws IOException {
        if (this.numOfEtries == this.maxNumOfEtries - 1 || this.IsIDInBLock(newEntry.getID())) {
            // TODO do an actual error managment
            throw new IOException();
        }
        this.numOfEtries++;
        this.entries.add(newEntry.getValues());

        this.indexOfEntries.add(new String(newEntry.getID()) + ":"+ addOldIndexToNewEntryIndexes(newEntry.getElementIndexes(), this.spaceInUse));
        this.spaceInUse = this.spaceInUse + newEntry.getEntrySize();

        if (this.indexOfEntries.get(0) == "0:0")
            this.indexOfEntries.remove(0);
        System.out.println(this.blockStats());
    }

    // ===========REMOVING_ENTRIES===============

    private String decrementDifference(String indexPartOfEntry, int diff) {
        String result = "";
        System.out.println(result);
        for (String index : indexPartOfEntry.split(":")[1].split(",")) {
            result = result + "," + String.valueOf(Integer.parseInt(index) - diff);
        }
        return indexPartOfEntry.split(":")[0] + ":" + result.substring(1);
    }

    private int sizeOfEntry(int indexOfEntry) {
        int start;
        int end;
        if (indexOfEntry == this.numOfEtries) {
            start = Integer.parseInt(
                    (this.indexOfEntries.get(indexOfEntry - 1).split(":")[1]).split(",")[this.numOfColumns - 1]);
            end = Integer
                    .parseInt((this.indexOfEntries.get(indexOfEntry).split(":")[1]).split(",")[this.numOfColumns - 1]);
        } else if (this.numOfEtries == 1) {
            start = 0;
            end = Integer.parseInt(
                    (this.indexOfEntries.get(indexOfEntry - 1).split(":")[1]).split(",")[this.numOfColumns - 1]);
        } else {
            start = Integer.parseInt((this.indexOfEntries.get(indexOfEntry).split(":")[1]).split(",")[0]);
            end = Integer.parseInt((this.indexOfEntries.get(indexOfEntry + 1).split(":")[1]).split(",")[0]);
        }
        return end - start;
    }

    private void removeIndexFromIndexOfEntries(int numOfEntryToRemove) {
        int size = this.sizeOfEntry(numOfEntryToRemove);
        this.spaceInUse = this.spaceInUse - size;
        if (numOfEntryToRemove + 1 == this.numOfEtries) {
            this.indexOfEntries.remove(numOfEntryToRemove);
        } else {
            for (int i = numOfEntryToRemove; i < this.numOfEtries - 1; i++) {
                this.indexOfEntries.set(i, decrementDifference(this.indexOfEntries.get(i + 1), size));
            }
            this.indexOfEntries.remove(this.numOfEtries - 1);
        }
    }

    public void removeEntry(byte[] entryID) throws IOException {
        if (!IsIDInBLock(entryID)) {
            // TODO kai kala.
            throw new IOException();
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
        System.out.println(this.blockStats());
    }
}
