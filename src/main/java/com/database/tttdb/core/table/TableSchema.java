package com.database.tttdb.core.table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.database.tttdb.api.Schema.ColumnInner;

public class TableSchema {
    private final String tableName;
    private final ColumnInner[] columns;

    private final String[] names;
    private final DataType[] types;
    private final int[] sizes;
    private final Object[] defaults;

    private final boolean[] indexed;
    private final boolean[] unique;
    private final boolean[] secondary;
    private final boolean[] autoIncrement;
    private final boolean[] notNull;

    private final Map<String, Integer> columnIndexMap;

    public final boolean hasPrimaryKey;
    public final boolean hasUnique;
    public final boolean hasIndex;

    public TableSchema(String tableName, ColumnInner[] columns) {
        this.tableName = tableName;
        this.columns = Arrays.copyOf(columns, columns.length);

        int len = columns.length;
        this.names = new String[len];
        this.types = new DataType[len];
        this.sizes = new int[len];
        this.defaults = new Object[len];
        this.indexed = new boolean[len];
        this.unique = new boolean[len];
        this.secondary = new boolean[len];
        this.autoIncrement = new boolean[len];
        this.notNull = new boolean[len];
        this.columnIndexMap = new HashMap<>(len);

        boolean pkFlag = false;
        boolean uqFlag = false;
        boolean idxFlag = false;

        for (int i = 0; i < len; i++) {
            ColumnInner c = columns[i];
            names[i] = c.name();
            types[i] = c.type();
            sizes[i] = c.size();
            defaults[i] = c.defaultValue();

            var cons = c.constraints();
            boolean pk = cons.contains(Constraint.PRIMARY_KEY);
            boolean uq = cons.contains(Constraint.UNIQUE);
            boolean sk = cons.contains(Constraint.SECONDARY_KEY);
            boolean ai = cons.contains(Constraint.AUTO_INCREMENT);
            boolean nn = pk || cons.contains(Constraint.NOT_NULL);

            indexed[i] = pk || uq || sk;
            unique[i] = uq;
            secondary[i] = sk;
            autoIncrement[i] = ai;
            notNull[i] = nn;

            if (pk) pkFlag = true;
            if (uq) uqFlag = true;
            if (sk) idxFlag = true;

            columnIndexMap.put(c.name(), i);
        }

        this.hasPrimaryKey = pkFlag;
        this.hasUnique = uqFlag;
        this.hasIndex = idxFlag;
    }

    // direct getters for precomputed arrays
    public String[] getNames() { return names; }
    public DataType[] getTypes() { return types; }
    public int[] getSizes() { return sizes; }
    public Object[] getDefaults() { return defaults; }
    public boolean[] isIndexed() { return indexed; }
    public boolean[] getUniqueIndex() { return unique; }
    public boolean[] getSecondaryIndex() { return secondary; }
    public boolean[] getAutoIncrementIndex() { return autoIncrement; }
    public boolean[] getNotNull() { return notNull; }

    public List<Constraint>[] getConstraints() {
        @SuppressWarnings("unchecked")
        List<Constraint>[] copy = new List[columns.length];
        for (int i = 0; i < columns.length; i++)
            copy[i] = List.copyOf(columns[i].constraints());
        return copy;
    }
    public int getPrimaryIndex() {
        for (int i = 0; i < columns.length; i++)
            if (columns[i].constraints().contains(Constraint.PRIMARY_KEY))
                return i;
        return -1;
    }

    public int numNullables() {
        int count = 0;
        for (boolean b : notNull) if (!b) count++;
        return count;
    }

    public int getNumOfColumns() { return columns.length; }

    public int getColumnIndex(String columnName) {
        return columnIndexMap.getOrDefault(columnName, -1);
    }

    public int getPreferredIndexColumn() {
        int pk = getPrimaryIndex();
        if (pk != -1) return pk;

        for (int i = 0; i < unique.length; i++)
            if (unique[i]) return i;
        for (int i = 0; i < secondary.length; i++)
            if (secondary[i]) return i;
        return 0;
    }

    @Override
    public String toString() {
        String[] headers = { "Column", "Type", "Size", "Constraints", "Default" };

        int maxNameLen = Math.max(headers[0].length(), Arrays.stream(names).mapToInt(String::length).max().orElse(0));
        int maxTypeLen = Math.max(headers[1].length(), Arrays.stream(types).mapToInt(t -> t.toString().length()).max().orElse(0));
        int maxSizeLen = Math.max(headers[2].length(), Arrays.stream(sizes).map(s -> String.valueOf(s).length()).max().orElse(0));
        int maxConstraintLen = headers[3].length();
        int maxDefaultLen = Math.max(headers[4].length(), Arrays.stream(defaults).mapToInt(v -> String.valueOf(v).length()).max().orElse(0));

        // compute max constraint length in one pass (constraints are in ColumnInner)
        for (ColumnInner c : columns)
            maxConstraintLen = Math.max(maxConstraintLen, c.constraints().toString().length());

        String border = "+"
            + "-".repeat(maxNameLen + 2)
            + "+"
            + "-".repeat(maxTypeLen + 2)
            + "+"
            + "-".repeat(maxSizeLen + 2)
            + "+"
            + "-".repeat(maxConstraintLen + 2)
            + "+"
            + "-".repeat(maxDefaultLen + 2)
            + "+";

        String banner = "TABLE: " + tableName.toUpperCase();
        int padding = Math.max(0, (border.length() - banner.length()) / 2);

        StringBuilder sb = new StringBuilder();
        sb.append(" ".repeat(padding)).append(banner).append("\n");
        sb.append(border).append("\n");

        // header row
        sb.append("| ").append(pad(headers[0], maxNameLen))
        .append(" | ").append(pad(headers[1], maxTypeLen))
        .append(" | ").append(pad(headers[2], maxSizeLen))
        .append(" | ").append(pad(headers[3], maxConstraintLen))
        .append(" | ").append(pad(headers[4], maxDefaultLen))
        .append(" |\n")
        .append(border).append("\n");

        // data rows
        for (int i = 0; i < columns.length; i++) {
            ColumnInner c = columns[i];
            String def = String.valueOf(defaults[i]);
            if (defaults[i] instanceof byte[] b)
                def = "byte[" + b.length + "]";

            sb.append("| ").append(pad(names[i], maxNameLen))
            .append(" | ").append(pad(types[i].toString(), maxTypeLen))
            .append(" | ").append(pad(String.valueOf(sizes[i]), maxSizeLen))
            .append(" | ").append(pad(c.constraints().toString(), maxConstraintLen))
            .append(" | ").append(pad(def, maxDefaultLen))
            .append(" |\n")
            .append(border).append("\n");
        }

        return sb.toString();
    }

    private static String pad(String s, int width) {
        if (s == null) s = "null";
        return s + " ".repeat(Math.max(0, width - s.length()));
    }
}