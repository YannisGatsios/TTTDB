package com.hashmap.db;

public class Table {
    private String Database;
    private String tableName;
    private String[] columnNames;
    private String[] columnTypes;
    private int[] columnSizes;

    private short numOfColumns;
    private int maxSizeOfEntry;
    private byte sizeOfIndexPerElement;
    private int numOfEntries;//TODO Needs the btree to fill with data.
    private int numOfBlocks;

    private short maxNumOfEntriesPerBlock;

    private String[] setColumnNames(String[] tableConfig){
        String[] result = new String[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = tableConfig[i].split(":")[0];
        }
        return result;
    }

    private String[] setColumnTypes(String[] tableConfig){
        String[] result = new String[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = tableConfig[i].split(":")[2];
        }
        return result;
    }

    private int[] setColumnSizes(String[] tableConfig){
        int[] result = new int[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = Integer.parseInt(tableConfig[i].split(":")[1]);
        }
        return result;
    }

    private int setMaxSizeOfEntry(){
        int sum = 0;
        for (int size : this.columnSizes) {
            sum = sum+size;
        }
        return sum;
    }

    public Table(String[] tableConfig){
        this.Database = tableConfig[0];
        this.tableName = tableConfig[0];
        this.columnNames = this.setColumnNames(tableConfig);
        this.columnTypes = this.setColumnTypes(tableConfig);
        this.columnSizes = this.setColumnSizes(tableConfig);

        this.maxSizeOfEntry = setMaxSizeOfEntry();
        this.numOfColumns = (short) this.columnSizes.length;
    }

    public String[] getColumnTypes(){
        return this.columnTypes;
    }

    public int getMaxSizeOfEntry(){
        return this.maxSizeOfEntry;
    }

    public short getNumOfColumns(){
        return this.numOfColumns;
    }

    public byte getSizeOfIndexPerElement(){
        return this.sizeOfIndexPerElement;
    }public void setSizeOfIndexPerElement(byte size){
        this.sizeOfIndexPerElement = size;
    }

    public void setMaxNumOfEntriesPerBlock(short maxNum){
        this.maxNumOfEntriesPerBlock = maxNum;
    } public short getMaxNumOfEntriesPerBlock(){
        return this.maxNumOfEntriesPerBlock;
    }
}
