package com.database.tttdb.core.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableSnapshot {
    private static class Snapshot {
        private Set<PageKey> deletedPageID;
        private int numOfPages;
        private Snapshot(Set<PageKey> deletedPageID, int numOfPages){
            this.deletedPageID = deletedPageID;
            this.numOfPages = numOfPages;
        }
    }
    private final List<Snapshot> snapshots = new ArrayList<>();
    public TableSnapshot(){
        this.snapshots.add(new Snapshot(new HashSet<>(), 0));
    }
    public void setNumOfPages(int numOfPages){
        this.snapshots.getLast().numOfPages = numOfPages;
    }
    public int getNumOfPages(){
        return this.snapshots.getLast().numOfPages;
    }
    public void addOnePage(){
        this.snapshots.getLast().numOfPages++;
    }
    public void removeOnePage(){
        if(this.snapshots.getLast().numOfPages == 0)
            throw new IllegalArgumentException("Can not remove one page. NumOfPages is already 0");
        this.snapshots.getLast().numOfPages--;
    }
    public Set<PageKey> getDeletedPageIDSet() { 
        return this.snapshots.getLast().deletedPageID;
    }
    public int getDeletedPages() { 
        return this.snapshots.getLast().deletedPageID.size();
    }
    public void clearDeletedPages(){
        this.snapshots.getLast().deletedPageID = new HashSet<>();
    }
    public void addDeletedPage(PageKey pageKey){
        this.snapshots.getLast().deletedPageID.add(pageKey);
    }
    public void removeDeletedPage(PageKey pageKey){
        this.snapshots.getLast().deletedPageID.remove(pageKey);
    }
    public void beginTransaction(){
        Snapshot last = this.snapshots.getLast();
        this.snapshots.add(new Snapshot(new HashSet<>(last.deletedPageID), last.numOfPages));
    }
    public void commit(){
        Snapshot committed = this.snapshots.removeLast();
        if(this.snapshots.isEmpty()){
            this.snapshots.add(new Snapshot(new HashSet<>(), 0));
            return;
        } 
        this.snapshots.getLast().deletedPageID = committed.deletedPageID;
        this.snapshots.getLast().numOfPages = committed.numOfPages;
    }
    public void rollback(){
        this.snapshots.removeLast();
        if(this.snapshots.isEmpty()) this.snapshots.add(new Snapshot(new HashSet<>(),0));
    }
}