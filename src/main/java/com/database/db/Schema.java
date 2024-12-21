package com.database.db;

public class Schema {

    private String[] columnNames;
    private String[] columnTypes;
    private int[] columnSizes;

    public Schema(String schema){
        this.columnNames = this.setColumnNames(schema.split(";"));
        this.columnTypes = this.setColumnTypes(schema.split(";"));
        this.columnSizes = this.setColumnSizes(schema.split(";"));
    }
        
    private String[] setColumnNames(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[0].trim();
        }
        return result;
    }

    private String[] setColumnTypes(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[2].trim();
        }
        return result;
    }

    private int[] setColumnSizes(String[] tableConfig){
        int[] result = new int[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = Integer.parseInt(tableConfig[i].split(":")[1].trim());
        }
        return result;
    }

    public String[] getColumnNames() {
        return this.columnNames;
    }

    public String[] getColumnTypes() {
        return this.columnTypes;
    }

    public int[] getColumnSizes() {
        return this.columnSizes;
    }
}
