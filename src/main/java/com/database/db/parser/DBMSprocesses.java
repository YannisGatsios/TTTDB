package com.database.db.parser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.index.BPlusTree;
import com.database.db.index.PrimaryKey;
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Schema;
import com.database.db.table.Table;

public class DBMSprocesses {
    private FileIO fileIO;
    public DBMSprocesses(FileIOThread fileIOThread){
        this.fileIO = new FileIO(fileIOThread);
    }
    //==SELECTING==
    public <K extends Comparable<K>> Entry<K> selectEntry(Table<K> table, K key) throws IOException, ExecutionException, InterruptedException {
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Integer BlockID = tree.search(key);
        if (BlockID != null){
            TablePage<K> page = new TablePage<>(BlockID, table);
            page.bufferToPage(fileIO.readPage(table.getTablePath(), page.getPagePos(), page.sizeInBytes()), table);
            return page.get(page.getIndex(key));
        }
        return null;
    }
    //==INSERTION==
    public <K extends Comparable<K>> void insertEntry(Table<K> table, Entry<K> entry) throws IOException, ExecutionException, InterruptedException {
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        TablePage<K> Page = new TablePage<>(tree.getNumberOfPages(), table);
        byte[] pageBuffer = fileIO.readPage(table.getTablePath(), Page.getPagePos(), Page.sizeInBytes());
        if(pageBuffer != null){
            Page.bufferToPage(pageBuffer, table);
        }
        if(Page.size() < table.getPageMaxNumOfEntries()){
            this.insertionProcess(table, entry, Page);
            return;
        }
        tree.addOnePage();
        Page = new TablePage<>(tree.getNumberOfPages(), table);
        this.insertionProcess(table, entry, Page);
    }
    private <K extends Comparable<K>> void insertionProcess(Table<K> table, Entry<K> entry, TablePage<K> Page) throws IOException{
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Page.add(entry);
        tree.insert(entry.getID(), (Integer) Page.getPageID());
        fileIO.writePage(table.getTablePath(), Page.toBuffer(), Page.getPagePos());
    }

    //==DELETION==
    public <K extends Comparable<K>> void deleteEntry(Table<K> table, K key) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        System.out.println(tree.getNumberOfPages());
        TablePage<K> page = this.deletionProcess(table, key);
        if (page == null) return;
        if (page.getPageID() == tree.getNumberOfPages() && page.size() != 0){
            fileIO.writePage(table.getTablePath(), page.toBuffer(), page.getPagePos());
            return;
        }else if (page.getPageID() == tree.getNumberOfPages() && page.size() == 0){
            fileIO.deleteLastPage(table.getTablePath(), page.sizeInBytes());
            tree.removeOnePage();
            return;
        }
        TablePage<K> lastPage = new TablePage<>(tree.getNumberOfPages(), table);
        lastPage.bufferToPage(fileIO.readPage(table.getTablePath(), lastPage.getPagePos(), lastPage.sizeInBytes()), table);
        Entry<K> lastEntry = lastPage.get(lastPage.size()-1);
        lastPage.remove(lastPage.size()-1);
        page.add(lastEntry);

        System.out.println(lastEntry.getID()+"--- added to Page : "+page.getPageID());
        tree.update(lastEntry.getID(), page.getPageID());
        fileIO.writePage(table.getTablePath(), page.toBuffer(), page.getPagePos());
        if(lastPage.size() == 0){
            tree.removeOnePage();
            fileIO.deleteLastPage(table.getTablePath(), lastPage.sizeInBytes());
            return;
        }
        fileIO.writePage(table.getTablePath(), lastPage.toBuffer(), lastPage.getPagePos());
    }
    private <K extends Comparable<K>> TablePage<K> deletionProcess(Table<K> table, K key) throws IOException, ExecutionException, InterruptedException {
        PrimaryKey<K> tree = table.getPrimaryKeyIndex();
        Integer BlockID = tree.search(key);
        if (BlockID != null){
            TablePage<K> page = new TablePage<>(BlockID, table);
            page.bufferToPage(fileIO.readPage(table.getTablePath(), page.getPagePos(), page.sizeInBytes()), table);
            if(page.getIndex(key) == -1){
                TablePage<K> page1 = new TablePage<>(1, table);
                page1.bufferToPage(fileIO.readPage(table.getTablePath(), page1.getPagePos(), page1.sizeInBytes()), table);
                int int1 = page1.getIndex(key);
                TablePage<K> page2 = new TablePage<>(2, table);
                page2.bufferToPage(fileIO.readPage(table.getTablePath(), page2.getPagePos(), page2.sizeInBytes()), table);
                int int2 = page2.getIndex(key);
                TablePage<K> page3 = new TablePage<>(0, table);
                page3.bufferToPage(fileIO.readPage(table.getTablePath(), page3.getPagePos(), page3.sizeInBytes()), table);
                int int3 = page3.getIndex(key);
                System.out.println("hello int1 : "+int3 +" int2 : "+int1+" int3 : "+int2+" Value is = "+tree.search(key));
            }
            page.remove(page.getIndex(key));
            tree.remove(key);
            return page;
        }
        return null;
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
