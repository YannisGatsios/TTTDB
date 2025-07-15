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
import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class DBMSprocesses {
    private FileIO fileIO;
    public DBMSprocesses(FileIOThread fileIOThread){
        this.fileIO = new FileIO(fileIOThread);
    }
    //==SELECTING==
    public <K extends Comparable<K>> Entry selectEntry(Table table, K key, int columnIndex) throws IOException, ExecutionException, InterruptedException {
        Integer BlockID = table.findBlock(key,columnIndex);
        if (BlockID != null){
            TablePage page = new TablePage(BlockID, table);
            return page.get(key, columnIndex);
        }
        return null;
    }
    //==INSERTION==
    public void insertEntry(Table table, Entry entry) throws IOException, ExecutionException, InterruptedException {
        if(table.getPages()==0)table.addOnePage();
        TablePage page = table.getCache().get(table.getPages()-1);
        if(page.size() < table.getEntriesPerPage()){
            this.insertionProcess(table, entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().get(table.getPages()-1);
        this.insertionProcess(table, entry, page);
    }
    private void insertionProcess(Table table, Entry entry, TablePage page) throws IOException{
        page.add(entry);
        table.insertIndex(entry, page.getPageID());
        page.write(table);
    }
    //==DELETION==
    public <K extends Comparable<K>> void deleteEntry(Table table, K key, int columnIndex) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        TablePage page = this.deletionProcess(table, key, columnIndex);
        if (page == null) return;
        if (page.getPageID() == table.getPages()-1 && page.size() != 0){
            page.write(table);
            return;
        }else if (page.getPageID() == table.getPages()-1 && page.size() == 0){
            fileIO.deleteLastPage(table.getPath(), page.sizeInBytes());
            table.removeOnePage();
            return;
        }
        TablePage lastPage = table.getCache().get(table.getPages()-1);
        Entry lastEntry = lastPage.removeLast();
        page.add(lastEntry);

        table.updateIndex(lastEntry, page.getPageID(), lastPage.getPageID());
        page.write(table);
        if(lastPage.size() == 0){
            fileIO.deleteLastPage(table.getPath(), lastPage.sizeInBytes());
            table.removeOnePage();
            return;
        }
        lastPage.write(table);
    }
    private <K extends Comparable<K>> TablePage deletionProcess(Table table, K key, int columnIndex) throws IOException, ExecutionException, InterruptedException {
        Integer BlockID = table.findBlock(key,columnIndex);
        if (BlockID != null){
            TablePage page = table.getCache().get(BlockID);
            Entry removedEntry = page.remove(key,columnIndex);
            table.removeIndex(removedEntry, BlockID);
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
    public void createTable(Table table){
        try {
            File tableFile = new File(table.getPath());
            if (tableFile.createNewFile()) {
                System.out.println("Table File created : " + tableFile.getName());
            } else {
                System.out.println("Table File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public void createPrimaryKey(Table table, int columnIndex)  throws InterruptedException, ExecutionException, IOException{
        try {
            File indexFile = new File(table.getPKPath());
            if (indexFile.createNewFile()) {
                System.out.println("Index File created : " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        if(table.getPrimaryKey() == null){
            table.initPrimaryKey(columnIndex);
        }
    }
    public void createIndex(Table table, int columnIndex){
        try {
            File indexFile = new File(table.getSKPath(columnIndex));
            if (indexFile.createNewFile()) {
                System.out.println("Index File created : " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        if(!table.getSchema().getSecondaryKey()[columnIndex]){

        }
    }
    //==DROPPING==
    public void dropDatabase(){
    }
    public void dropTable(Table table){
        Path tablePath = Paths.get(table.getPath());
        Path primaryKeyPath = Paths.get(table.getPKPath());
        int[] indexList = table.getSchema().getSecondaryKeyIndex();
        try {
            Files.delete(tablePath);
            System.out.println("Table File deleted successfully.");
            Files.delete(primaryKeyPath);
            System.out.println("Primary Key File deleted successfully.");
            for (int i : indexList) {
                Path secondaryKeyPath = Paths.get(table.getSKPath(i)); 
                Files.delete(secondaryKeyPath);
                System.out.println("Secondary Key File deleted successfully.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while deleting a Table or Index file.");
            e.printStackTrace();
        }
    }
    public void dropIndex(){
    }
}
