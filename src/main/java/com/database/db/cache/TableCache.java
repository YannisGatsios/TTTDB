package com.database.db.cache;

import com.database.db.Database;
import com.database.db.page.IndexPage;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class TableCache {
    private final Table table;
    private final Database database;
    public TableCache(Table table, Database database){
        this.table = table;
        this.database = database;
    }
    public TablePage getTablePage(int pageID){
        String pageKey = table.getName()+"."+pageID;
        return database.getCache().getTablePage(pageKey);
    }
    public IndexPage getIndexPage(int pageID, int columnIndex){
        String columnName = table.getSchema().getNames()[columnIndex];
        String pageKey = table.getName()+"."+columnName+"."+pageID;
        return database.getCache().getIndexPage(pageKey);
    }

    public void putTablePage(TablePage page){
        String pageKey = table.getName()+"."+page.getPageID();
        database.getCache().put(pageKey, page);
    }
    public void putIndexPage(IndexPage page){
        String columnName = table.getSchema().getNames()[page.getColumnIndex()];
        String pageKey = table.getName()+"."+columnName+"."+page.getPageID();
        database.getCache().put(pageKey, page);
    }

    public TablePage getLastTablePage(){
        return database.getCache().getLastTablePage(table);
    }
    public IndexPage getLastIndexPage(int columnIndex){
        return database.getCache().getLastIndexPage(table, columnIndex);
    }

    public void deleteLastTablePage(TablePage page){
        String pageKey = table.getName()+"."+page.getPageID();
        database.getCache().deleteLastTablePage(pageKey, table, page);
    }
    public void deleteLastIndexPage(IndexPage page){
        String columnName = table.getSchema().getNames()[page.getColumnIndex()];
        String pageKey = table.getName()+"."+columnName+"."+page.getPageID();
        database.getCache().deleteLastIndexPage(pageKey, table, page);
    }
}