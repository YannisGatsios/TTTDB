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

    public BPlusTree insertionProcess(Table table, Entry entry, BPlusTree tree) throws IOException{
        Page Page = new Page(tree.getLastPageID(), table);
        byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeOfPage());
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

    public BPlusTree delete(Table table, BPlusTree tree, Object key) throws IllegalArgumentException,IOException{
        Pair<?,?> pair = tree.findPair(key);
        if(pair == null) throw new IllegalArgumentException("The key you are tring to delete is not found.");

        if((int)pair.getValue() == tree.getLastPageID()){
            Page Page = new Page((int)pair.getValue(), table);
            byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeOfPage());
            Page = Page.bufferToPage(pageBuffer, table);
            int index =  Page.getIndex(key);
            Page.remove(index);
            tree.remove(key);
            Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        }
        Page Page = new Page(tree.getLastPageID(), table);
        Page = Page.bufferToPage(Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeOfPage()), table);
        Entry lastEntry = Page.get(Page.size()-1);
        Page.remove(Page.getIndex(lastEntry.getID()));
        tree.remove(lastEntry.getID());
        Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());

        Page = new Page((int)pair.getValue(), table);
        byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeOfPage());
        Page = Page.bufferToPage(pageBuffer, table);
        int index =  Page.getIndex(key);
        Page.remove(index);
        tree.remove(key);
        Page.add(lastEntry);
        Pair<?,?> newPair = new Pair<>(lastEntry.getID(), pair.getValue());
        tree.insert(newPair);
        Page.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        return tree;
    }

    public void update(){

    }
}
