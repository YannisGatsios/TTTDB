package com.database.db;

public class Schema {

    private String[] columnNames;
    private int[] columnSizes;
    private String[] columnTypes;
    private boolean[] nullable;
    private boolean[] isSecodaryKey;
    private boolean[] isPrimaryKey;
    private final int numOfColumns;

    private static int MAX_SIZE_OF_COLUMN_NAME = 20;

    public Schema(String[] schema){
        this.columnNames = this.setColumnNames(schema);
        this.columnSizes = this.setColumnSizes(schema);
        this.columnTypes = this.setColumnTypes(schema);
        this.nullable = this.setNullable(schema);
        this.isSecodaryKey = this.setSecodaryKey(schema);
        this.isPrimaryKey = this.setPrimaryKey(schema);
        this.numOfColumns = nullable.length;
    }
    
    private String[] setColumnNames(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[0].trim();
        }
        return result;
    }public String[] getColumnNames() {
        return this.columnNames;
    }

    private int[] setColumnSizes(String[] tableConfig){
        int[] result = new int[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Integer.parseInt(tableConfig[i].split(":")[1].trim());
        }
        return result;
    }public int[] getColumnSizes() {
        return this.columnSizes;
    }

    private String[] setColumnTypes(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[2].trim();
        }
        return result;
    }public String[] getColumnTypes() {
        return this.columnTypes;
    }

    private boolean[] setNullable(String[] tableConfig){
        boolean result[] = new boolean[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Boolean.parseBoolean(tableConfig[i].split(":")[3].trim());
        }
        return result;
    }public boolean[] getNullable(){
        return this.nullable;
    }

    private boolean[] setSecodaryKey(String[] tableConfig){
        boolean result[] = new boolean[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Boolean.parseBoolean(tableConfig[i].split(":")[4].trim());
        }
        return result;
    }public boolean[] getSecodaryKey(){
        return this.isSecodaryKey;
    }

    private boolean[] setPrimaryKey(String[] tableConfig){
        boolean result[] = new boolean[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Boolean.parseBoolean(tableConfig[i].split(":")[5].trim());
        }
        return result;
    }public boolean[] getPrimaryKey(){
        return this.isPrimaryKey;
    }

    public int getPrimaryKeyIndex(){
        int ind = -1;
        for (boolean element : this.isPrimaryKey) {
            if(element == true){
                ind++;
                return ind;
            }
            ind++;
        }
        return ind;
    }

    public void printSchema(){
        String[] valueNames = {"Name :", "Buffer Size :", "Data Type :", "Nullable :", "Secodary Key :", "Primary Key :"};
        String[] result = new String[this.numOfColumns+1];
        result[0] = "| ";
        for (int i = 0; i < valueNames.length; i++) {
            result[0] += valueNames[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-valueNames[i].length())+"| ";
        }
        for(int i = 0; i < this.numOfColumns; i++){
            result[i+1] = "| "+this.columnNames[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-columnNames[i].length())+"| "
            +this.columnSizes[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.columnSizes[i]).length())+"| "
            +this.columnTypes[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-columnTypes[i].length())+"| "
            +this.nullable[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.nullable[i]).length())+"| "
            +this.isSecodaryKey[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.isSecodaryKey[i]).length())+"| "
            +this.isPrimaryKey[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.isPrimaryKey[i]).length())+"| ";
        }
        String border = "+"+String.valueOf("-").repeat(result[0].length()-3)+"+";
        System.out.println(String.valueOf(" ").repeat(border.length()/2- 20)+"!===========! TABLE_SCHEMA !===========!\n"+border);
        for (String string : result) {
            System.out.println(string);
            System.out.println(border);
        }
    }
}
