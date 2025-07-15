package com.database.db.table;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private String[] columnNames;
    private int[] columnSizes;
    private Type[] columnTypes;
    private boolean[] nullable;
    private boolean[] isSecondaryKey;
    private boolean[] isPrimaryKey;
    private final int numOfColumns;

    private static int MAX_SIZE_OF_COLUMN_NAME = 20;

    public Schema(String[] schema){
        this.columnNames = this.setNames(schema);
        this.columnTypes = this.setTypes(schema);
        this.columnSizes = this.setSizes(schema);
        this.nullable = this.setNullable(schema);
        this.isSecondaryKey = this.setSecondaryKey(schema);
        this.isPrimaryKey = this.setPrimaryKey(schema);
        this.numOfColumns = nullable.length;
    }
    
    private String[] setNames(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[0].trim();
        }
        return result;
    }public String[] getNames() {
        return this.columnNames;
    }

    private int[] setSizes(String[] tableConfig){
        int[] result = new int[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = this.columnTypes[i].getFixedSize();
            if(result[i] == -1)result[i] = Integer.parseInt(tableConfig[i].split(":")[1].trim());
        }
        return result;
    }public int[] getSizes() {
        return this.columnSizes;
    }

    private Type[] setTypes(String[] tableConfig){
        Type[] result = new Type[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Type.fromString(tableConfig[i].split(":")[2].trim());
        }
        return result;
    }public Type[] getTypes() {
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

    private boolean[] setSecondaryKey(String[] tableConfig){
        boolean result[] = new boolean[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Boolean.parseBoolean(tableConfig[i].split(":")[4].trim());
        }
        return result;
    }public boolean[] getSecondaryKey(){
        return this.isSecondaryKey;
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
        for (int i = 0;i < this.numOfColumns;i++) {
            if(this.isPrimaryKey[i])return i;
        }
        return -1;
    }

    public int[] getSecondaryKeyIndex(){
        List<Integer> resultList = new ArrayList<>();
        for (int i = 0; i < this.numOfColumns; i++) {
            if (this.isSecondaryKey[i]) {
                resultList.add(i);
            }
        }
        int[] result = new int[resultList.size()];
        for (int j = 0; j < resultList.size(); j++) {
            result[j] = resultList.get(j);
        }

        return result;
    }

    @Override
    public String toString(){
        String[] valueNames = {"Name :", "Buffer Size :", "Data Type :", "Nullable :", "Secondary Key :", "Primary Key :"};
        String[] columns = new String[this.numOfColumns+1];
        columns[0] = "| ";
        for (int i = 0; i < valueNames.length; i++) {
            columns[0] += valueNames[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-valueNames[i].length())+"| ";
        }
        for(int i = 0; i < this.numOfColumns; i++){
            columns[i+1] = "| "+this.columnNames[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-columnNames[i].length())+"| "
            +this.columnSizes[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.columnSizes[i]).length())+"| "
            +this.columnTypes[i].toString()+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-columnTypes[i].getFixedSize())+"| "
            +this.nullable[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.nullable[i]).length())+"| "
            +this.isSecondaryKey[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.isSecondaryKey[i]).length())+"| "
            +this.isPrimaryKey[i]+String.valueOf(" ").repeat(MAX_SIZE_OF_COLUMN_NAME-String.valueOf(this.isPrimaryKey[i]).length())+"| ";
        }
        String border = "+"+String.valueOf("-").repeat(columns[0].length()-3)+"+";
        String result = String.valueOf(" ").repeat(border.length()/2- 20)+"!===========! TABLE_SCHEMA !===========!\n"+border+"\n"; 
        for (String string : columns) {
            result = result+string+"\n"+
            border+"\n";
        }
        return result;
    }
}
