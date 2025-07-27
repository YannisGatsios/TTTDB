package com.database.db.manager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.database.db.table.Schema;
import com.database.db.table.Table;

public class SchemaManager {

    public static void createDatabase() {
        // TODO: implement creating a database folder or similar
    }

    public static void createTable(Schema schema, String path, String databaseName, String tableName) {
        try {
            File tableFile = new File(path+databaseName+"."+tableName+".table");
            if (tableFile.createNewFile()) {
                System.out.println("Table File created: " + tableFile.getName());
            } else {
                System.out.println("Table File already exists.");
            }
            String[] columnNames = schema.getNames();
            boolean[] isIndexed = schema.isIndexed();
            for (int i = 0;i<columnNames.length;i++) {
                if(isIndexed[i]){
                    SchemaManager.createIndex(path+databaseName+"."+tableName+"."+columnNames[i]+".index");
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the table.");
            e.printStackTrace();
        }
    }

    public static void createIndex(String path) {
        try {
            File indexFile = new File(path);
            if (indexFile.createNewFile()) {
                System.out.println("Index File created: " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the index.");
            e.printStackTrace();
        }
    }

    public static void dropDatabase() {
        // TODO: implement dropping the database
    }

    public static void dropTable(Table table) {
        Path tablePath = Paths.get(table.getPath());
        boolean[] isIndexed = table.getSchema().isIndexed();
        try {
            Files.delete(tablePath);
            System.out.println("Table File deleted successfully.");
            for (int i = 0;i<isIndexed.length;i++) {
                if(isIndexed[i]){
                    Path secondaryKeyPath = Paths.get(table.getIndexPath(i));
                    Files.delete(secondaryKeyPath);
                    System.out.println("Secondary Key File deleted successfully.");
                }
            }
        } catch (IOException e) {
            System.out.println("An error occurred while deleting a Table or Index file.");
            e.printStackTrace();
        }
    }

    public static void dropIndex() {
        // TODO: implement drop index
    }
}