package com.database.tttdb.core.index;


import com.database.tttdb.core.page.Page;
import com.database.tttdb.core.page.TablePage;
import com.database.tttdb.core.table.Table;

public class SecondaryKey<K extends Comparable<? super K>> extends IndexInit<K> {
    public SecondaryKey(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.setUnique(false);
        this.setNullable(true);
        this.columnIndex = columnIndex;
    }
}