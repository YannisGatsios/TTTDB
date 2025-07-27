package com.database.db.table;

import java.util.ArrayList;
import java.util.List;

public class Schema {

    private Column[] columns;
    
    public boolean hasPrimaryKey = false;
    public boolean hasUnique = false;
    public boolean hasIndex = false;

    public record Column(String name, DataType type, int size, List<Constraint> constraints, Object Default) {}

    public Schema(String[] schema) throws Exception{
        this.columns = this.getColumns(schema);
    }

    private Column[] getColumns(String[] columnConfigs){
        Column[] result = new Column[columnConfigs.length]; 
        for (int i = 0;i<result.length;i++) {
            result[i] = this.createColumn(columnConfigs[i]);
        }
        return result;
    }

    private Column createColumn(String columnConfig) {
        String[] fields = columnConfig.split(":");
        String columnName = fields[0].trim();
        DataType columnDataType = DataType.fromString(fields[1].trim());
        int size = columnDataType.getSize();
        int columnSize = size == -1 ? Integer.parseInt(fields[2].trim()) : size;
        String[] constraintStrings = (fields[3].trim()).split(",");
        ArrayList<Constraint> columnConstraints = new ArrayList<>();
        for (String constraintString : constraintStrings) {
            Constraint constraint = Constraint.fromString(constraintString);
            if (constraint == null)
                throw new IllegalArgumentException("Invalid Constraint from column " + columnName);
            switch (constraint) {
                case PRIMARY_KEY -> this.hasPrimaryKey = true;
                case UNIQUE -> this.hasUnique = true;
                case INDEX -> this.hasIndex = true;
                default -> {}
            }
            columnConstraints.add(constraint);
        }
        Object Default = columnDataType.parseValue(fields[4].trim());
        return new Column(columnName, columnDataType, columnSize, columnConstraints, Default);
    }
    public String[] getNames(){
        String[] names = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            names[i] = columns[i].name();
        }
        return names;
    }
    public DataType[] getTypes() {
    DataType[] types = new DataType[columns.length];
    for (int i = 0; i < columns.length; i++) {
        types[i] = columns[i].type();
        }
        return types;
    }
    public int[] getSizes() {
        int[] sizes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sizes[i] = columns[i].size();
        }
        return sizes;
    }
    public List<Constraint>[] getConstraints() {
        @SuppressWarnings("unchecked")
        List<Constraint>[] constraints = (List<Constraint>[]) new List[columns.length];
        for (int i = 0; i < columns.length; i++) {
            constraints[i] = columns[i].constraints();
        }
        return constraints;
    }
    public Object[] getDefaults() {
        Object[] defaults = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            defaults[i] = columns[i].Default();
        }
        return defaults;
    }

    public boolean[] isIndexed(){
        boolean[] result = new boolean[this.columns.length];
        for (int i = 0; i < columns.length; i++) {
            result[i] = (columns[i].constraints.contains(Constraint.PRIMARY_KEY) || columns[i].constraints.contains(Constraint.UNIQUE) || columns[i].constraints.contains(Constraint.INDEX));
        }
        return result;
    }

    public int getPrimaryKeyIndex(){
        int index = 0;
        for (Column column : this.columns) {
            if(column.constraints.contains(Constraint.PRIMARY_KEY)) return index;
            index++;
        }
        return -1;
    }

    public boolean[] getUniqueIndex(){
        boolean[] result = new boolean[this.columns.length];
        for (int i = 0;i<this.columns.length;i++) {
            Column column = this.columns[i];
            result[i] = (column.constraints.contains(Constraint.UNIQUE));
        }
        return result;
    }

    public boolean[] getIndexIndex(){
        boolean[] result = new boolean[this.columns.length];
        for (int i = 0;i<this.columns.length;i++) {
            Column column = this.columns[i];
            result[i] = (column.constraints.contains(Constraint.INDEX));
        }
        return result;
    }

    public boolean[] getAutoIncrementIndex(){
        boolean[] result = new boolean[this.columns.length];
        for (int i = 0;i<this.columns.length;i++) {
            Column column = this.columns[i];
            result[i] = (column.constraints.indexOf(Constraint.AUTO_INCREMENT) != -1);
        }
        return result;
    }

    public boolean[] getNotNull() {
        boolean[] result = new boolean[columns.length];
        for (int i = 0; i < columns.length; i++) {
            var c = columns[i];
            result[i] = c.constraints().contains(Constraint.PRIMARY_KEY) ||
                    c.constraints().contains(Constraint.NOT_NULL);
        }
        return result;
    }

    public int numNullables() {
        boolean[] notNull = getNotNull();
        int count = 0;
        for (boolean b : notNull) {
            if (!b)
                count++;
        }
        return count;
    }

    public int getNumOfColumns(){return this.columns.length;}

    @Override
    public String toString() {
        String[] headers = { "Column Name", "Type", "Size", "Constraints", "Default" };

        int maxNameLen = headers[0].length();
        int maxTypeLen = headers[1].length();
        int maxSizeLen = headers[2].length();
        int maxConstraintLen = headers[3].length();
        int maxDefaultLen = headers[4].length();

        // compute max widths in one loop
        for (Column c : columns) {
            maxNameLen = Math.max(maxNameLen, c.name().length());
            maxTypeLen = Math.max(maxTypeLen, c.type().toString().length());
            maxSizeLen = Math.max(maxSizeLen, String.valueOf(c.size()).length());
            maxConstraintLen = Math.max(maxConstraintLen, c.constraints().toString().length());
            maxDefaultLen = Math.max(maxDefaultLen, String.valueOf(c.Default()).length());
        }

        String border = "+" + "-".repeat(maxNameLen + 2) +
                "+" + "-".repeat(maxTypeLen + 2) +
                "+" + "-".repeat(maxSizeLen + 2) +
                "+" + "-".repeat(maxConstraintLen + 2) +
                "+" + "-".repeat(maxDefaultLen + 2) + "+";

        String banner = "TABLE SCHEMA";
        int padding = Math.max(0, (border.length() - banner.length()) / 2);

        StringBuilder sb = new StringBuilder();
        sb.append(" ".repeat(padding)).append(banner).append("\n");
        sb.append(border).append("\n");

        // header row
        sb.append("| ")
                .append(pad(headers[0], maxNameLen)).append(" | ")
                .append(pad(headers[1], maxTypeLen)).append(" | ")
                .append(pad(headers[2], maxSizeLen)).append(" | ")
                .append(pad(headers[3], maxConstraintLen)).append(" | ")
                .append(pad(headers[4], maxDefaultLen)).append(" |\n");

        sb.append(border).append("\n");

        // data rows
        for (Column c : columns) {
            sb.append("| ")
                    .append(pad(c.name(), maxNameLen)).append(" | ")
                    .append(pad(c.type().toString(), maxTypeLen)).append(" | ")
                    .append(pad(String.valueOf(c.size()), maxSizeLen)).append(" | ")
                    .append(pad(c.constraints().toString(), maxConstraintLen)).append(" | ")
                    .append(pad(String.valueOf(c.Default()), maxDefaultLen)).append(" |\n");
            sb.append(border).append("\n");
        }

        return sb.toString();
    }

    private String pad(String s, int width) {
        if (s == null)
            s = "null"; // safeguard
        return s + " ".repeat(Math.max(0, width - s.length()));
    }

}
