package com.database.db;

public class Table {
    private String Database;
    private String tableName;
    private String[] columnNames;
    private String[] columnTypes;
    private int[] columnSizes;

    private short numOfColumns;
    private int maxSizeOfEntry;
    private int numOfEntries;//TODO Needs the btree to fill with data.
    private int numOfBlocks;

    private short maxNumOfEntriesPerBlock;

    private String[] setColumnNames(String[] tableConfig){
        String[] result = new String[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = tableConfig[i].split(":")[0].trim();
        }
        return result;
    }

    private String[] setColumnTypes(String[] tableConfig){
        String[] result = new String[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = tableConfig[i].split(":")[2].trim();
        }
        return result;
    }

    private int[] setColumnSizes(String[] tableConfig){
        int[] result = new int[tableConfig.length-2];
        for (int i = 2;i<tableConfig.length;i++){
            result[i-2] = Integer.parseInt(tableConfig[i].split(":")[1].trim());
        }
        return result;
    }

    private int setMaxSizeOfEntry(){
        int sum = 0;
        for (int i = 0;i < this.numOfColumns; i++) {
            if(i == 0){
                sum += this.columnSizes[i];
            }else if(this.columnTypes[i].equals("String")){
                sum += this.columnSizes[i] + Short.BYTES;
            }else if(this.columnTypes[i].equals("Integer")){
                sum += Integer.BYTES;
            }else if(this.columnTypes[i].equals("Byte")){
                sum += this.columnSizes[i] + Short.BYTES;
            }else{
                throw new IllegalArgumentException("Invalid Element Type For Entry.");
            }
        }
        return sum;
    }

    public Table(String[] tableConfig){
        this.Database = tableConfig[0];
        this.tableName = tableConfig[1];
        this.columnNames = this.setColumnNames(tableConfig);
        this.columnTypes = this.setColumnTypes(tableConfig);
        this.columnSizes = this.setColumnSizes(tableConfig);

        this.numOfColumns = (short) this.columnSizes.length;
        this.maxSizeOfEntry = this.setMaxSizeOfEntry();
    }

    public String getDatabase(){
        return this.Database;
    }

    public String getTableName(){
        return this.tableName;
    }

    public String[] getColumnTypes(){
        return this.columnTypes;
    }

    public int getMazSizeOfID(){
        return this.columnSizes[0];
    }

    public int getMaxSizeOfEntry(){
        return this.maxSizeOfEntry;
    }

    public short getNumOfColumns(){
        return this.numOfColumns;
    }

    public int[] getColumnSizes(){
        return this.columnSizes;
    }
    
    public void setMaxNumOfEntriesPerBlock(short maxNum){
        this.maxNumOfEntriesPerBlock = maxNum;
    } public short getMaxNumOfEntriesPerBlock(){
        return this.maxNumOfEntriesPerBlock;
    }
}
