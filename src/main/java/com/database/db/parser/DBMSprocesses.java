package com.database.db.parser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.FileIOThread;
import com.database.db.index.BPlusTree;
import com.database.db.index.BTreeSerialization.BlockPointer;
import com.database.db.page.TablePage;
import com.database.db.table.Constraint;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class DBMSprocesses {
    private FileIO fileIO;
    public DBMSprocesses(FileIOThread fileIOThread){
        this.fileIO = new FileIO(fileIOThread);
    }
    //==SELECTING==
    public <K extends Comparable<K>> List<Entry> selectEntry(Table table, K key, int columnIndex) throws IOException, ExecutionException, InterruptedException {
        List<Entry> result = new ArrayList<>();
        List<BlockPointer> blockPointerList = table.findEntry(key, columnIndex);
        for (BlockPointer blockPointer : blockPointerList) {
            TablePage page = table.getCache().get(blockPointer.BlockID());
            result.add(page.get(blockPointer.RowOffset()));
        }
        return result;
    }
    //==INSERTION==
    public void insertEntry(Table table, Entry entry) throws IOException, ExecutionException, InterruptedException,Exception {
        table.isValidEntry(entry);
        if(table.getPages()==0)table.addOnePage();
        TablePage page = table.getCache().getLast();
        if(page.size() < table.getPageCapacity()){
            this.insertionProcess(table, entry, page);
            return;
        }
        table.addOnePage();
        page = table.getCache().getLast();
        this.insertionProcess(table, entry, page);
    }
    private void insertionProcess(Table table, Entry entry, TablePage page) throws IOException{
        page.add(entry);
        table.insertIndex(entry, new BlockPointer(page.getPageID(), (short)(page.size()-1)));
    }
    //==DELETION==
    public <K extends Comparable<K>> List<Entry> deleteEntry(Table table, K key, int columnIndex) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException{
        return this.deleteEntry(table, key, columnIndex,-1);
    }
    public <K extends Comparable<K>> List<Entry> deleteEntry(Table table, K key, int columnIndex, int limit) throws IllegalArgumentException,IOException, ExecutionException , InterruptedException {
        ArrayList<Entry> result = new ArrayList<>();
        int index = -1;
        List<BlockPointer> blockPointerList = table.findEntry(key,columnIndex);
        boolean deleteAll = limit < 0;
        for (BlockPointer blockPointer : blockPointerList) {
            index++;
            if(!deleteAll && index>=limit)return result;
            TablePage page = table.getCache().get(blockPointer.BlockID());
            if (page == null) continue;
            result.add(this.deletionProcess(table, page, blockPointer));
            if (page.isLastPage() && page.size() == 0) {
                fileIO.deleteLastPage(table.getPath(), page.sizeInBytes());
                table.removeOnePage();
                continue;
            }
            if(page.isLastPage()) continue;
            TablePage lastPage = table.getCache().getLast();
            Entry lastEntry = lastPage.removeLast();
            BlockPointer oldPointer = new BlockPointer(lastPage.getPageID(), (short)(lastPage.size()));
            page.add(lastEntry);
            table.updateIndex(lastEntry, blockPointer, oldPointer);
            if (lastPage.size() == 0) {
                fileIO.deleteLastPage(table.getPath(), lastPage.sizeInBytes());
                table.removeOnePage();
            }
        }
        return result;
    }
    private <K extends Comparable<K>> Entry deletionProcess(Table table, TablePage page, BlockPointer blockPointer) throws IOException, ExecutionException, InterruptedException {
        Entry removed = page.remove(blockPointer.RowOffset());
        table.removeIndex(removed, blockPointer);
        if(page.size() > 0 && blockPointer.RowOffset() != page.size()){
            Entry swapped = page.get(blockPointer.RowOffset());//On removal last entry gets the place of the removed one
            BlockPointer oldValue = new BlockPointer(page.getPageID(), page.size());
            table.updateIndex(swapped, blockPointer, oldValue);
        }
        return removed;
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
            File indexFile = new File(table.getIndexPath(columnIndex));
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
            File indexFile = new File(table.getIndexPath(columnIndex));
            if (indexFile.createNewFile()) {
                System.out.println("Index File created : " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        if(table.getSchema().getConstraints()[columnIndex].indexOf(Constraint.INDEX) == -1){

        }
    }
    //==DROPPING==
    public void dropDatabase(){
    }
    public void dropTable(Table table){
        Path tablePath = Paths.get(table.getPath());
        Path primaryKeyPath = Paths.get(table.getIndexPath(table.getPrimaryKeyColumnIndex()));
        int[] indexList = table.getSchema().getIndexIndex();
        try {
            Files.delete(tablePath);
            System.out.println("Table File deleted successfully.");
            Files.delete(primaryKeyPath);
            System.out.println("Primary Key File deleted successfully.");
            for (int i : indexList) {
                Path secondaryKeyPath = Paths.get(table.getIndexPath(i)); 
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
