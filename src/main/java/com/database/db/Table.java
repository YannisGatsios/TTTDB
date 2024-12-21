package com.database.db;

public class Table {
    private String Database;
    private String tableName;
    private Schema tablSchema;

    private short numOfColumns;
    private int maxSizeOfEntry;
    private int numOfEntries;//TODO Needs the btree to fill with data.
    private int numOfBlocks;

    private short maxNumOfEntriesPerBlock;

    public Table(String databaseName, String tableName,Schema schema){
        this.Database = databaseName;
        this.tableName = tableName;
        this.tablSchema = schema;
        this.numOfColumns = (short) this.tablSchema.getColumnSizes().length;
        this.maxSizeOfEntry = this.setMaxSizeOfEntry();
    }

    private int setMaxSizeOfEntry(){
        int sum = 0;
        for (int i = 0;i < this.numOfColumns; i++) {
            if(i == 0){
                sum += this.tablSchema.getColumnSizes()[i];
            }else if(this.tablSchema.getColumnTypes()[i].equals("String")){
                sum += this.tablSchema.getColumnSizes()[i] + Short.BYTES;
            }else if(this.tablSchema.getColumnTypes()[i].equals("Integer")){
                sum += Integer.BYTES;
            }else if(this.tablSchema.getColumnTypes()[i].equals("Byte")){
                sum += this.tablSchema.getColumnSizes()[i] + Short.BYTES;
            }else{
                throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        return sum;
    }

    public String getDatabase(){
        return this.Database;
    }

    public String getTableName(){
        return this.tableName;
    }

    public Schema getSchema(){
        return this.tablSchema;
    }

    public int getMaxSizeOfEntry(){
        return this.maxSizeOfEntry;
    }

    public short getNumOfColumns(){
        return this.numOfColumns;
    }

    public int getMaxIDSize(){
        return this.tablSchema.getColumnSizes()[0];
    }
    
    public void setMaxNumOfEntriesPerBlock(short maxNum){
        this.maxNumOfEntriesPerBlock = maxNum;
    } public short getMaxNumOfEntriesPerBlock(){
        return this.maxNumOfEntriesPerBlock;
    }
}
