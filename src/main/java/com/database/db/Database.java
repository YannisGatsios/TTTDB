package com.database.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.api.DBMS;
import com.database.db.api.DBMS.TableConfig;
import com.database.db.api.ForeignKey.ForeignKeyAction;
import com.database.db.cache.Cache;
import com.database.db.cache.TransactionCache;
import com.database.db.api.ForeignKey;
import com.database.db.api.Schema;
import com.database.db.manager.SchemaManager;
import com.database.db.table.Table;

public class Database {
    private static final Logger logger = Logger.getLogger(Database.class.getName());

    private final String name;
    private DBMS dbms;
    private String path = "";
    private final Map<String,Table> tables;
    private final Map<String,Schema> schema;
    private final Cache mainCache;
    private TransactionCache currentCache;

    public Database(String name, DBMS dbms, int cacheCapacity){
        this.name = name;
        this.dbms = dbms;
        this.tables = new HashMap<>();
        this.schema = new HashMap<>();
        this.mainCache = new Cache(this, cacheCapacity);
    }

    public void create(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String tableName : tableNames) {
            Table table = tables.get(tableName);
            SchemaManager.createTable(table.getSchema(), this.path, this.name, tableName);
            table.start();
        }
    }

    public void createTable(TableConfig tableConfig) {
        if (this.tables.containsKey(tableConfig.tableName())) {
            logger.info(String.format("Table '%s' already exists in database '%s'. Skipping creation.", tableConfig.tableName(), this.name));
            return;
        }
        this.addTable(tableConfig);
        logger.info(
            String.format("Table '%s' created successfully in database '%s'.", tableConfig.tableName(), this.name));
    }
    private void addTable(TableConfig tableConfig){
        Schema newSchema = tableConfig.schema();
        Table newTable = new Table(this, tableConfig);
        this.prepareForeignKey(newTable, newSchema);
        this.schema.put(tableConfig.tableName(), newSchema);
        this.tables.put(tableConfig.tableName(), newTable);
    }
    public record TableReference(
        String childTable, 
        String parentTable,
        List<String> childColumns, 
        List<String> parentColumns,
        ForeignKeyAction onUpdate, 
        ForeignKeyAction onDelete
    ){}
    private void prepareForeignKey(Table childTable, Schema schema){
        if(schema.getForeignKeys().isEmpty()) return;
        List<ForeignKey> foreignKeyList = schema.getForeignKeys();
        for (ForeignKey foreignKey : foreignKeyList) {
            Table parentTable = tables.get(foreignKey.getReferenceTable());
            TableReference tableReference = new TableReference(
                childTable.getName(), 
                parentTable.getName(), 
                foreignKey.getChildColumns(), 
                foreignKey.getReferenceColumns(), 
                foreignKey.getOnUpdate(), 
                foreignKey.getOnDelete());
            parentTable.addChild(tableReference);
            childTable.addParent(tableReference);
        }
    }

    public void close(){
        for (Table table : this.tables.values()) {
            try {
                table.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.WARNING,
                    String.format("InterruptedException: Shutdown interrupted while closing table '%s' in database '%s'.", table.getName(), this.name), e);
            }
        }
        logger.info(String.format("All tables closed for database '%s'.", this.name));
    }

    public void dropDatabase(){
        this.commit();
        this.removeAllTables();
    }
    public void removeAllTables(){
        Set<String> tableNames = new HashSet<>(this.tables.keySet());
        for (String string : tableNames) {
            this.dropTable(string);
        }
        logger.info(String.format("Removed %d table(s) from database '%s'.", tableNames.size(), this.name));
    }
    public void dropTable(String tableName){
        Table table = this.getTable(tableName);
        if (table == null) {
            logger.warning(String.format("Warning: Tried to remove non-existent table '%s' from database '%s'.", tableName, this.name));
            return;
        }
        try {
            SchemaManager.dropTable(table);
            table.close();
            this.tables.remove(tableName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log(Level.WARNING,
                String.format("InterruptedException: Shutdown interrupted while removing table '%s' from database '%s'.", tableName, this.name), e);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                String.format("Error removing table '%s' from database '%s'.",tableName, name),e);
        }
        logger.info(String.format("Table '%s' removed from database '%s'.", tableName, this.name));
    }

    public void startTransaction(String name){
        if(this.currentCache == null) this.currentCache = new TransactionCache(this, this.mainCache, name);
        else this.currentCache = new TransactionCache(this, this.currentCache, name);
    }
    public void rollBack(){
        if(this.currentCache == null){
            this.mainCache.rollback();
            return;
        }
        this.currentCache.rollback();
        Cache parent = this.currentCache.getParent();
        if(parent instanceof TransactionCache) this.currentCache = (TransactionCache)parent;
        else this.currentCache = null;
    }
    public Database commit(){
        if(this.currentCache == null) {
            this.mainCache.commit();
            return this;
        }
        this.currentCache.commit();
        Cache parent = this.currentCache.getParent();
        if(parent instanceof TransactionCache) this.currentCache = (TransactionCache)parent;
        else this.currentCache = null;
        return this;
    }

    public Cache getCache(){
        if(this.currentCache == null) return this.mainCache;
        return this.currentCache;
    }
    public DBMS getDBMS() {return this.dbms;}
    public Table getTable(String tableName) {return tables.get(tableName);}
    public Schema getSchema(String tableName) {return schema.get(tableName);}
    public void setPath(String path){this.path = path;}
    public String getPath() { return this.path; }
    public String getName(){ return this.name; }
    public List<Table> getAllTablesList() {
        return new ArrayList<>(tables.values());
    }
}