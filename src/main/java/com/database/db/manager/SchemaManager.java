package com.database.db.manager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;

import com.database.db.table.Constraint;
import com.database.db.table.Table;

public class SchemaManager {

    public static void createDatabase() {
        // TODO: implement creating a database folder or similar
    }

    public static void createTable(Table table) {
        try {
            File tableFile = new File(table.getPath());
            if (tableFile.createNewFile()) {
                System.out.println("Table File created: " + tableFile.getName());
            } else {
                System.out.println("Table File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the table.");
            e.printStackTrace();
        }
    }

    public static void createPrimaryKey(Table table, int columnIndex) 
            throws InterruptedException, ExecutionException, IOException {
        try {
            File indexFile = new File(table.getIndexPath(columnIndex));
            if (indexFile.createNewFile()) {
                System.out.println("Index File created: " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the primary key.");
            e.printStackTrace();
        }
        if (table.getSchema().hasPrimaryKey) {
            table.initPrimaryKey(columnIndex);
        }
    }

    public static void createIndex(Table table, int columnIndex) {
        try {
            File indexFile = new File(table.getIndexPath(columnIndex));
            if (indexFile.createNewFile()) {
                System.out.println("Index File created: " + indexFile.getName());
            } else {
                System.out.println("Index File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating the index.");
            e.printStackTrace();
        }
        if (table.getSchema().getConstraints()[columnIndex].indexOf(Constraint.INDEX) == -1) {
            // TODO: actually set the constraint
        }
    }

    public static void dropDatabase() {
        // TODO: implement dropping the database
    }

    public static void dropTable(Table table) {
        Path tablePath = Paths.get(table.getPath());
        Path primaryKeyPath = Paths.get(table.getIndexPath(table.getSchema().getPrimaryKeyIndex()));
        boolean[] indexList = table.getSchema().getIndexIndex();
        try {
            Files.delete(tablePath);
            System.out.println("Table File deleted successfully.");
            Files.delete(primaryKeyPath);
            System.out.println("Primary Key File deleted successfully.");
            for (int i = 0;i<indexList.length;i++) {
                if(indexList[i]){
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