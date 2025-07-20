package com.database.db.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Schema {

    private String[] columnNames;
    private String[] columnParameters;
    private Type[] columnTypes;
    private int[] columnSizes;
    private List<Constraint>[] columnConstraints;
    private String[] columnDefault;
    
    public boolean hasPrimaryKey = false;
    public boolean hasUnique = false;
    public boolean hasIndex = false;

    private static final int MAX_CONSTRAINTS_PER_COLUMN = 4;

    public Schema(String[] schema) throws Exception{
        this.columnNames = this.setNames(schema);
        this.columnParameters = this.setParams(schema);
        this.columnSizes = new int[schema.length];
        this.columnTypes = this.setTypes(schema);
        this.columnConstraints = this.setConstraints(schema);
        this.columnDefault = this.setDefault(schema);
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
    
    private String[] setParams(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[2];
        }
        return result;
    }

    private Type[] setTypes(String[] tableConfig){
        Type[] result = new Type[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            Type type = Type.fromString(tableConfig[i].split(":")[1].trim());
            if(type.getSize() == -1) this.columnSizes[i] = Integer.parseInt(this.columnParameters[i]);
            else this.columnSizes[i] = type.getSize();
            result[i] = type;
        }
        return result;
    }public Type[] getTypes() {
        return this.columnTypes;
    }
    public int[] getSizes() {
        return this.columnSizes;
    }
    @SuppressWarnings("unchecked")
    private ArrayList<Constraint>[] setConstraints(String[] tableConfig) throws Exception{
        ArrayList<Constraint>[] result = new ArrayList[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            int index = 0;
            ArrayList<Constraint> list = new ArrayList<>();
            for(String constraintString : tableConfig[i].split(":")[3].trim().split(",")){
                if(index > MAX_CONSTRAINTS_PER_COLUMN) 
                    throw new Exception("Number of Constraints must not be bigger than "+MAX_CONSTRAINTS_PER_COLUMN);
                Constraint constraint = Constraint.fromString(constraintString);
                if(constraint==null) throw new IllegalArgumentException("Invalid Constraint from column "+this.columnNames[i]);
                switch (constraint) {
                    case PRIMARY_KEY -> this.hasPrimaryKey = true;
                    case UNIQUE -> this.hasUnique = true;
                    case INDEX -> this.hasIndex = true;
                    default -> {}
                }
                list.add(constraint);
                index++;
            }
            result[i] = list;
        }
        return result;
    }
    public List<Constraint>[] getConstraints(){return this.columnConstraints;}

    private String[] setDefault(String[] tableConfig){
        String[] result = new String[tableConfig.length];
        for (int i = 0;i<tableConfig.length;i++){
            result[i] = tableConfig[i].split(":")[4].trim();
        }
        return result;
    }
    public String[] getDefaults(){return this.columnDefault;}

    public int getPrimaryKeyIndex(){
        int index = 0;
        for (List<Constraint> constraintList : columnConstraints) {
            if(constraintList.indexOf(Constraint.PRIMARY_KEY) != -1) return index;
            index++;
        }
        return -1;
    }

    public int[] getUniqueIndex(){
        List<Integer> resultList = new ArrayList<>();
        int index = 0;
        for (List<Constraint> constraintList : columnConstraints) {
            if(constraintList.indexOf(Constraint.UNIQUE) != -1) resultList.add(index);
            index++;
        }
        int[] result = new int[resultList.size()];
        for (int j = 0; j < resultList.size(); j++) {
            result[j] = resultList.get(j);
        }
        return result;
    }

    public int[] getAutoIncrementIndex(){
        List<Integer> resultList = new ArrayList<>();
        int index = 0;
        for (List<Constraint> constraintList : columnConstraints) {
            if(constraintList.indexOf(Constraint.AUTO_INCREMENT) != -1) resultList.add(index);;
            index++;
        }
        int[] result = new int[resultList.size()];
        for (int j = 0; j < resultList.size(); j++) {
            result[j] = resultList.get(j);
        }
        return result;
    }

    public int[] getIndexIndex(){
        List<Integer> resultList = new ArrayList<>();
        int index = 0;
        for (List<Constraint> constraintList : columnConstraints) {
            if(constraintList.indexOf(Constraint.INDEX) != -1) resultList.add(index);;
            index++;
        }
        int[] result = new int[resultList.size()];
        for (int j = 0; j < resultList.size(); j++) {
            result[j] = resultList.get(j);
        }
        return result;
    }

    public boolean[] getNotNull(){
        boolean[] result = new boolean[this.columnNames.length];
        int index = 0;
        for (List<Constraint> constraintList : columnConstraints) {
            result[index] = (constraintList.indexOf(Constraint.PRIMARY_KEY) != -1 || constraintList.indexOf(Constraint.NOT_NULL) != -1);
            index++;
        }
        return result;
    }

    public int getNumOfColumns(){return this.columnNames.length;}

    public int numNullables(){
        boolean[] notNull = this.getNotNull();
        int count = 0;
        for (boolean b : notNull) {
            if (!b) count++;
        }
        return count;
    }

    @Override
    public String toString(){
        String[] headers = {"Column Name :", "Type :", "Size :", "Constraints :", "Default :"};
        String[] columns = new String[this.columnNames.length+1];
        columns[0] = "| ";

        int maxNameLen = Math.max(
                Arrays.stream(columnNames).mapToInt(String::length).max().orElse(0),
                headers[0].length());
        int maxTypeLen = 9;
        int maxSizeLen = Math.max(
                Arrays.stream(columnSizes).map(s -> String.valueOf(s).length()).max().orElse(0),
                headers[2].length());
        int maxConstraintLen = Math.max(
                Arrays.stream(columnConstraints)
                        .map(list -> list.toString())
                        .mapToInt(String::length)
                        .max()
                        .orElse(0),
                headers[3].length());
        int maxDefaultLen = Math.max(
                Arrays.stream(columnDefault).mapToInt(String::length).max().orElse(0),
                headers[4].length());
            String border =
            "+" + "-".repeat(maxNameLen + 2) +
            "+" + "-".repeat(maxTypeLen + 2) +
            "+" + "-".repeat(maxSizeLen + 2) +
            "+" + "-".repeat(maxConstraintLen + 2) +
            "+" + "-".repeat(maxDefaultLen + 2) + "+";

            String banner = "TABLE SCHEMA";
            int padding = Math.max(0, (border.length() - banner.length()) / 2);

            StringBuilder sb = new StringBuilder();
            sb.append(" ".repeat(padding))
                    .append(banner).append("\n")
                    .append(border).append("\n");
            // Header row
            sb.append("| ")
                    .append(pad(headers[0], maxNameLen)).append(" | ")
                    .append(pad(headers[1], maxTypeLen)).append(" | ")
                    .append(pad(headers[2], maxSizeLen)).append(" | ")
                    .append(pad(headers[3], maxConstraintLen)).append(" | ")
                    .append(pad(headers[4], maxDefaultLen)).append(" |\n")
                    .append(border).append("\n");

            for (int i = 0; i < columnNames.length; i++) {
                sb.append("| ")
                        .append(pad(columnNames[i], maxNameLen)).append(" | ")
                        .append(pad(columnTypes[i].toString(), maxTypeLen)).append(" | ")
                        .append(pad(String.valueOf(columnSizes[i]), maxSizeLen)).append(" | ")
                        .append(pad(String.valueOf(columnConstraints[i]), maxConstraintLen)).append(" | ")
                        .append(pad(columnDefault[i], maxDefaultLen)).append(" |\n")
                        .append(border).append("\n");
            }

            return sb.toString();
        }
        private String pad(String s, int width) {
            return s + " ".repeat(Math.max(0, width - s.length()));
        }
}
