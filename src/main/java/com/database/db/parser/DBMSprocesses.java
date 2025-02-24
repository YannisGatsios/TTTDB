package com.database.db.parser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.database.db.FileIO;
import com.database.db.index.BPlusTree;
import com.database.db.index.PrimaryKey;
import com.database.db.page.Page;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class DBMSprocesses {
    private static FileIO fileIO = new FileIO();
    public DBMSprocesses(){
    }
    //==SELECTING==
    public <K extends Comparable<K>> List<Entry<K>> select(){
        return null;//TODO
    }
    //==INSERTION==
    public <K extends Comparable<K>> void insertionProcess(Table table, Entry<K> entry) throws IOException{
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Page<K> Page = new Page<>(tree.getNumberOfPages(), table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        if(pageBuffer != null){
            Page = Page.bufferToPage(pageBuffer, table);
        }
        if(Page.size() < table.getPageMaxNumOfEntries()){
            this.insertionSteps(table, entry, Page);
            return;
        }
        tree.addOnePage();
        Page = new Page<>(tree.getNumberOfPages(), table);
        this.insertionSteps(table, entry, Page);
    }
    private <K extends Comparable<K>> void insertionSteps(Table table, Entry<K> entry, Page<K> Page) throws IOException{
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Page.add(entry);
        tree.insert(entry.getID(), (Integer) (Integer)Page.getPageID());
        fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
    }

    //==DELETION==
    public <K extends Comparable<K>> PrimaryKey<K> deletionProcess(Table table, K key) throws IllegalArgumentException,IOException{
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Integer value = tree.search(key);
        if(value == null) throw new IllegalArgumentException("The key you are trying to delete is not found.(From DeletionProcess)");

        if(value == tree.getNumberOfPages()){
            this.deleteSteps(value, table, key);
            return tree;
        }
        //Getting the last entry from the last page.
        Page<K> Page = new Page<>(tree.getNumberOfPages(), table);
        Page = Page.bufferToPage(fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes()), table);
        Entry<K> lastEntry = Page.get(Page.size() - 1);
        //Removing the last entry from the last Page recorded.
        this.deleteSteps(tree.getNumberOfPages(), table, lastEntry.getID());
        //Removing the Entry user asked for and replacing it the one deleted one above.
        Page = new Page<>(value, table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        if(pageBuffer == null || pageBuffer.length == 0){
            System.out.println("empty page");
        }
        Page = Page.bufferToPage(pageBuffer, table);
        int ind =  Page.getIndex(key);
        if( ind == -1){
            tree.search(key);
        }
        int index =  Page.getIndex(key);
        if (index == -1) throw new IllegalArgumentException("The key you are trying to find dose not exist in this Page.\nKey : "+key+"\nPageID : "+Page.getPageID());
        Page.remove(index);
        tree.remove(key);
        Page.add(lastEntry);
        tree.insert(lastEntry.getID(), value);
        fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        return tree;
    }
    private <K extends Comparable<K>> void deleteSteps(int value, Table table, K key) throws IOException{
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Page<K> Page = new Page<>(value, table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        Page = Page.bufferToPage(pageBuffer, table);
        int index = Page.getIndex(key);
        if (index == -1) throw new IllegalArgumentException("The key you are trying to find dose not exist in this Page.\nKey : "+key+"\nPageID : "+Page.getPageID()+"\nValue : "+value);
        Page.remove(index);
        tree.remove(key);
        fileIO.writePage(table.getTablePath(), Page.pageToBuffer(Page), Page.getPagePos());
        if (Page.size() == 0) {
            fileIO.deleteLastPage(table.getTablePath(), Page.sizeInBytes());
            tree.removeOnePage();
        }
    }

    //==UPDATING==
    public <K extends Comparable<K>> BPlusTree<K,Integer> update(){
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
