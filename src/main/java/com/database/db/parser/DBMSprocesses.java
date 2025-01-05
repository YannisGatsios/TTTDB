package com.database.db.parser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.database.db.bPlusTree.Tree;
import com.database.db.FileIO;
import com.database.db.bPlusTree.BPlusTree;
import com.database.db.page.Page;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class DBMSprocesses<K extends Comparable<K>> {
    private static FileIO fileIO = new FileIO();
    public DBMSprocesses(){
    }
    //==SELECTING==
    public List<Entry<K>> select(){
        return null;//TODO
    }
    //==INSERTION==
    public BPlusTree<K,Integer> insertionProcess(Table table, Entry<K> entry, BPlusTree<K,Integer> tree) throws IOException{
        Page<K> Page = new Page<>(tree.getLastPageID(), table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        if(pageBuffer != null){
            Page = Page.bufferToPage(pageBuffer, table);
        }
        if(Page.size() < table.getPageMaxNumOfEntries()){
            return this.insertionSteps(table, tree, entry, Page);
        }
        tree.addOnePageID();
        Page = new Page<>(tree.getLastPageID(), table);
        return this.insertionSteps(table, tree, entry, Page);
    }
    
    private BPlusTree<K,Integer> insertionSteps(Table table, BPlusTree<K,Integer> tree, Entry<K> entry, Page<K> Page) throws IOException{
        Page.add(entry);
        tree.insert(entry.getID(), (Integer) (Integer)Page.getPageID());
        fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        return tree;
    }

    //==DELETION==
    public BPlusTree<K,Integer> deletionProcess(Table table, BPlusTree<K,Integer> tree, K key) throws IllegalArgumentException,IOException{
        Integer value = tree.search(key);
        if(value == null) throw new IllegalArgumentException("The key you are trying to delete is not found.(From DeletionProcess)");

        if(value == tree.getLastPageID()){
            Page<K> Page = new Page<>(value, table);
            byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
            Page = Page.bufferToPage(pageBuffer, table);
            int index =  Page.getIndex(key);
            Page.remove(index);
            tree.remove(key);
            fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
            if(Page.size() == 0){
                fileIO.deleteLastPage(table.getTablePath(), Page.sizeInBytes());
                tree.removeOnePageID();
            }
            return tree;
        }
        //Removing the last entry from the last Page recorded.
        Page<K> Page = new Page<>(tree.getLastPageID(), table);
        Page = Page.bufferToPage(fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes()), table);
        Entry<K> lastEntry = Page.get(Page.size() - 1);//Keeping the removed entry for later.
        Page.remove(Page.getIndex(lastEntry.getID()));
        tree.remove(lastEntry.getID());
        fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        if(Page.size() == 0){
            fileIO.deleteLastPage(table.getTablePath(), Page.sizeInBytes());
            tree.removeOnePageID();
        }

        //Removing the Entry user asked for and replacing it the one deleted one above.
        Page<K> Page2 = new Page<>(value, table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page2.getPagePos(), Page2.sizeInBytes());
        Page2 = Page2.bufferToPage(pageBuffer, table);
        int ind =  Page2.getIndex(key);
        if( ind == -1){
            tree.search(key);
        }
        int index =  Page2.getIndex(key);
        Page2.remove(index);
        tree.remove(key);
        Page2.add(lastEntry);
        tree.insert(lastEntry.getID(), value);
        fileIO.writePage(table.getTablePath(), Page2.pageToBuffer(Page2), Page2.getPagePos());
        return tree;
    }
    //==UPDATING==
    public Tree<K,Integer> update(){
        return null;//TODO
    }

    //==CREATING==
    public void createDatabase(){
    }
    public void createTable(String databaseName, String tableName, Schema schema){
        try {
            File tableFile = new File("storage/"+databaseName+"."+tableName+".table");
            if (tableFile.createNewFile()) {
                System.out.println("Table File created : " + tableFile.getName());
            } else {
                System.out.println("Table File already exists.");
            }
            File indexFile = new File("storage/index/"+databaseName+"."+tableName+".index");
            if (indexFile.createNewFile()) {
                System.out.println("Index File created : " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public void createIndex(){
    }
    //==DROPPING==
    public void dropDatabase(){
    }
    public void dropTable(String databaseName, String tableName){
        Path tablePath = Paths.get("storage/"+databaseName+"."+tableName+".table");
        Path indexPath = Paths.get("storage/index/"+databaseName+"."+tableName+".index");
        try {
            Files.delete(tablePath);
            System.out.println("Table File deleted successfully.");
            Files.delete(indexPath);
            System.out.println("Index File deleted successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred while deleting a Table or Index file.");
            e.printStackTrace();
        }
    }
    public void dropIndex(){
    }
}
