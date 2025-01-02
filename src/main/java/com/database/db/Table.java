package com.database.db;

import java.util.ArrayList;

public class Table {
    private String Database;
    private String tableName;
    private Schema tablSchema;

    private short numOfColumns;
    private int sizeOfEntry;
    private int IDindex;
    private static int BLOCK_SIZE = 4096;
    private static int SIZE_OF_HEADER = 2 * (Integer.BYTES) + Short.BYTES;

    private short maxEntriesPerPage;

    public Table(String databaseName, String tableName,Schema schema){
        this.Database = databaseName;
        this.tableName = tableName;
        this.tablSchema = schema;
        
        this.numOfColumns = (short) this.tablSchema.getColumnSizes().length;
        this.sizeOfEntry = this.setSizeOfEntry();
        this.maxEntriesPerPage = this.setMaxEntriesPerPage();
        this.IDindex = this.tablSchema.getPrimaryKeyIndex();
    }

    private int setSizeOfEntry(){
        int sum = 0;
        String type[] = this.tablSchema.getColumnTypes();
        int size[] = this.tablSchema.getColumnSizes();
        for (int i = 0;i < this.numOfColumns; i++) {
            if(i == 0){
                sum += size[i];
            }else if(type[i].equals("String")){
                sum += size[i] + Short.BYTES;
            }else if(type[i].equals("Integer")){
                sum += Integer.BYTES;
            }else if(type[i].equals("Byte")){
                sum += size[i] + Short.BYTES;
            }else{
                throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        return sum;
    }

    public boolean isValidEntry(Entry entry){
        //Check if the number of columns matches the number of elements in the entry.
        if (entry.getEntry().size() != this.numOfColumns) {
            return false;
        }

        // Get the entry data and check each element's type and size.
        ArrayList<Object> entryData = entry.getEntry();
        String[] expectedTypes = this.tablSchema.getColumnTypes();
        int[] expectedSizes = this.tablSchema.getColumnSizes();

        for (int i = 0; i < this.numOfColumns; i++) {
            Object value = entryData.get(i);
            String expectedType = expectedTypes[i];
            int expectedSize = expectedSizes[i];

            // Validate type and size of each entry element.
            if (!isValidType(value, expectedType) && i != 0) {
                return false;
            }if (!isValidSize(value, expectedSize)) {
                return false;
            }
        }

        // All checks passed, return true.
        return true;
    }

    // Helper method to check if the type of the element matches the expected type.
    private boolean isValidType(Object value, String expectedType) {
        switch (expectedType) {
            case "String":
                return value instanceof String;
            case "Integer":
                return value instanceof Integer;
            case "Byte":
                return value instanceof byte[];
            default:
                return false; // Invalid type in schema.
        }
    }

    // Helper method to check if the size of the element is within the expected
    // size.
    private boolean isValidSize(Object value, int expectedSize) {
        if (value instanceof String) {
            return ((String) value).length() <= expectedSize;
        } else if (value instanceof Integer) {
            return Integer.BYTES == expectedSize; // Integer size is always 4 bytes.
        } else if (value instanceof byte[]) {
            return ((byte[]) value).length <= expectedSize;
        }
        return false;
    }

    public int getNumOfColumns(){
        return this.numOfColumns;
    }

    public String getTablePath(){
        return "storage/"+this.getDatabaseName()+"."+this.getTableName()+".table";
    }

    public String getDatabaseName(){
        return this.Database;
    }

    public String getTableName(){
        return this.tableName;
    }

    public Schema getSchema(){
        return this.tablSchema;
    }

    public int getSizeOfEntry(){
        return this.sizeOfEntry;
    }

    public int getMaxIDSize(){
        return this.tablSchema.getColumnSizes()[0];
    }

    public short setMaxEntriesPerPage(){
        if(!((BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry >= 3)){
            return this.setMaxEntriesPerPage(2);
        }
        return (short) ((BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry);
    }private short setMaxEntriesPerPage(int numOfPages){
        if(!((numOfPages*(BLOCK_SIZE-SIZE_OF_HEADER))/this.sizeOfEntry >= 3)){
            return this.setMaxEntriesPerPage(numOfPages + 1);
        }
        return (short) (numOfPages * (BLOCK_SIZE-SIZE_OF_HEADER)/this.sizeOfEntry);
    }public short getMaxEntriesPerPage(){
        return this.maxEntriesPerPage;
    }

    public int getIDindex(){
        return this.IDindex;
    }
}
