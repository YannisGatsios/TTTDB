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
        //TODO not sure if i need to write the updated tree for the moment i just return it. 
        Page Page = new Page(tree.getLastPageID(), table.getMaxEntriesPerPage());
        Page.setMaxSizeOfEntry(table.getSizeOfEntry());
        byte[] pageBuffer = Page.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeOfPage());
        if(pageBuffer != null){
            Page = Page.bufferToPage(pageBuffer, table);
        }

        if(Page.getNumOfEntries() < table.getMaxEntriesPerPage()){
            this.insertionSteps(table, tree, entry, Page, pageBuffer);
            return tree;
        }
        tree.addOnePageID();
        Page = new Page(tree.getLastPageID(), table.getMaxEntriesPerPage());
        Page.setMaxSizeOfEntry(table.getSizeOfEntry());
        this.insertionSteps(table, tree, entry, Page, pageBuffer);
        return tree;
    }
    private void insertionSteps(Table table, BPlusTree tree, Entry entry, Page Page, byte[] pageBuffer) throws IOException{
        Page.addEntry(entry);
        Pair<?,Integer> pair = new Pair<>(entry.getID(), Page.getPageID());
        pageBuffer = Page.pageToBuffer(Page);
        Page.writePage(table.getTablePath(), pageBuffer, Page.getPagePos());
        tree.insert(pair);
        System.out.println("Inseted Key = "+pair.getKey()+" : Value = "+pair.getValue());
    }


    public void delete(){

    }

    public void update(){

    }
}
