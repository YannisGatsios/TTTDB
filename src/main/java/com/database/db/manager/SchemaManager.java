package com.database.db.manager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.database.db.table.SchemaInner;
import com.database.db.table.Table;

public class SchemaManager {
    private static final Logger logger = Logger.getLogger(SchemaManager.class.getName());
    
    public static void createTable(SchemaInner schema, String path, String databaseName, String tableName) {
        File tableFile = new File(path + databaseName + "." + tableName + ".table");
        try {
            if(!tableFile.createNewFile()){
                logger.fine("Table file already existed: " + tableFile.getName());
            }

            String[] columnNames = schema.getNames();
            boolean[] isIndexed = schema.isIndexed();

            for (int i = 0; i < columnNames.length; i++) {
                if (isIndexed[i]) {
                    SchemaManager.createIndex(path + databaseName + "." + tableName + "." + columnNames[i] + ".index");
                }
            }
            logger.fine("Table and Index Files created: " + tableFile.getName());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create table file at path: " + tableFile.getPath(), e);
        }
    }

    public static void createIndex(String path) {
        File indexFile = new File(path);
        try {
            if(!indexFile.createNewFile()){
                logger.fine("Index file already existed: " + indexFile.getName());
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to create index file at path: " + indexFile.getPath(), e);
        }
    }

    public static void dropTable(Table table) {
        Path tablePath = Paths.get(table.getPath());
        boolean[] isIndexed = table.getSchema().isIndexed();
        try {
            Files.delete(tablePath);
            
            for (int i = 0; i < isIndexed.length; i++) {
                if (isIndexed[i]) {
                    Path secondaryKeyPath = Paths.get(table.getIndexPath(i));
                    Files.delete(secondaryKeyPath);
                }
            }
            logger.fine("Table and Index Files deleted successfully: " + tablePath);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "An error occurred while deleting a Table or Index file.", e);
        }
    }
}