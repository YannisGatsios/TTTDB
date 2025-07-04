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
        Integer BlockID = table.getPrimaryKey().search(key).value;
        if (BlockID != null){
            TablePage<K> page = new TablePage<>(BlockID, table);
            return page.get(key);
        }
        return null;
    }
    //==INSERTION==
    public <K extends Comparable<K>> void insertEntry(Table<K> table, Entry<K> entry) throws IOException, ExecutionException, InterruptedException {
        if(table.getPages()==0)table.addOnePage();
        TablePage<K> page = new TablePage<>(table.getPages()-1, table);
        if(page.size() < table.getEntriesPerPage()){
            this.insertionProcess(table, entry, page);
            return;
        }
        table.addOnePage();
        page = new TablePage<>(table.getPages()-1, table);
        this.insertionProcess(table, entry, page);
    }
    private <K extends Comparable<K>> void insertionProcess(Table<K> table, Entry<K> entry, TablePage<K> page) throws IOException{
        PrimaryKey<K> tree = table.getPrimaryKey();
        page.add(entry);
        tree.insert(entry.getID(), (Integer) page.getPageID());
        page.write(table);
    }
    //==DELETION==
    public <K extends Comparable<K>> void deleteEntry(Table<K> table, K key) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        TablePage<K> page = this.deletionProcess(table, key);
        if (page == null) return;
        if (page.getPageID() == table.getPages()-1 && page.size() != 0){
            page.write(table);
            return;
        }else if (page.getPageID() == table.getPages()-1 && page.size() == 0){
            fileIO.deleteLastPage(table.getTablePath(), page.sizeInBytes());
            table.removeOnePage();
            return;
        }
        TablePage<K> lastPage = new TablePage<>(table.getPages()-1, table);
        Entry<K> lastEntry = lastPage.getLast();
        lastPage.removeLast();
        page.add(lastEntry);

        table.getPrimaryKey().update(lastEntry.getID(), page.getPageID());
        page.write(table);
        if(lastPage.size() == 0){
            fileIO.deleteLastPage(table.getTablePath(), lastPage.sizeInBytes());
            table.removeOnePage();
            return;
        }
        lastPage.write(table);
    }
    private <K extends Comparable<K>> TablePage<K> deletionProcess(Table<K> table, K key) throws IOException, ExecutionException, InterruptedException {
        PrimaryKey<K> tree = table.getPrimaryKey();
        Integer BlockID = tree.search(key).value;
        if (BlockID != null){
            TablePage<K> page = new TablePage<>(BlockID, table);
            page.remove(key);
            tree.remove(key, BlockID);
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
