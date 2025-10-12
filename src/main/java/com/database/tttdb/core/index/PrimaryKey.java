package com.database.tttdb.core.index;


import com.database.tttdb.core.page.Page;
import com.database.tttdb.core.page.TablePage;
import com.database.tttdb.core.table.Table;

public class PrimaryKey<K extends Comparable<? super K>> extends IndexInit<K> {
    public PrimaryKey(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.columnIndex = columnIndex;
    }
}
