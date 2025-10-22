package com.database.tttdb.core.index;

import com.database.tttdb.core.table.Table;

public class SecondaryKey<K extends Comparable<? super K>> extends IndexInit<K> {
    public SecondaryKey(Table table, int columnIndex) {
        super(table.getDatabase().getIndexType());
        this.setUnique(false);
        this.setNullable(true);
        this.columnIndex = columnIndex;
    }
}