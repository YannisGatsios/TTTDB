package com.database.db.parser;

import java.io.IOException;

import com.database.db.Entry;
import com.database.db.Table;
import com.database.db.bPlusTree.BPlusTree;
import com.database.db.bPlusTree.TreeUtils.Pair;
import com.database.db.page.Page;

public class DBMSprocesses {

    public DBMSprocesses(){

    }
    
    public void select(){

    }

    //==INSERION==
    public BPlusTree insertionProcess(Table table, Entry entry, BPlusTree tree) throws IOException{
        Page Page = new Page(tree.getLastPageID(), table);
        byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        if(pageBuffer != null){
            Page = Page.bufferToPage(pageBuffer, table);
        }

        if(Page.size() < table.getMaxEntriesPerPage()){
            return this.insertionSteps(table, tree, entry, Page);
        }
        tree.addOnePageID();
        Page = new Page(tree.getLastPageID(), table);
        return this.insertionSteps(table, tree, entry, Page);
    }
    private BPlusTree insertionSteps(Table table, BPlusTree tree, Entry entry, Page Page) throws IOException{
        Page.add(entry);
        Pair<?,Integer> pair = new Pair<>(entry.getID(), Page.getPageID());
        tree.insert(pair);
        Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        return tree;
    }

    //==DELETION==
    public BPlusTree delete(Table table, BPlusTree tree, Object key) throws IllegalArgumentException,IOException{
        Pair<?,?> pair = tree.findPair(key);
        if(pair == null) throw new IllegalArgumentException("The key you are tring to delete is not found.(From DeletionProcess)");

        if((int)pair.getValue() == tree.getLastPageID()){
            Page Page = new Page((int)pair.getValue(), table);
            byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
            Page = Page.bufferToPage(pageBuffer, table);
            int index =  Page.getIndex(key);
            Page.remove(index);
            tree.remove(key);
            Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
            if(Page.size() == 0){
                Page.deleteLastPage(table.getTablePath(), Page.sizeInBytes());
                tree.removeOnePageID();
            }
            return tree;
        }
        //Removing the last entry from the last Page recorded.
        Page Page = new Page(tree.getLastPageID(), table);
        Page = Page.bufferToPage(Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes()), table);
        Entry lastEntry = Page.get(Page.size() - 1);//Keeping the removed entry for later.
        Page.remove(Page.getIndex(lastEntry.getID()));
        tree.remove(lastEntry.getID());
        Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        if(Page.size() == 0){
            Page.deleteLastPage(table.getTablePath(), Page.sizeInBytes());
            tree.removeOnePageID();
        }

        //Removing the Entry user asked for and replacing it the one deleted one above.
        Page Page2 = new Page((int)pair.getValue(), table);
        byte[] pageBuffer = Page2.readPage(table.getTablePath(), Page2.getPagePos(), Page2.sizeInBytes());
        Page2 = Page2.bufferToPage(pageBuffer, table);
        int ind =  Page2.getIndex(key);
        if( ind == -1){
            tree.findPair(key);
        }
        int index =  Page2.getIndex(key);
        Page2.remove(index);
        tree.remove(key);
        Page2.add(lastEntry);
        Pair<?,?> newPair = new Pair<>(lastEntry.getID(), pair.getValue());
        tree.insert(newPair);
        Page2.writePage(table.getTablePath(), Page2.pageToBuffer(Page2), Page2.getPagePos());
        return tree;
    }

    public void update(){

    }
}
