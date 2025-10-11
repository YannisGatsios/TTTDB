package com.database.db.core.index;


import com.database.db.core.page.Page;
import com.database.db.core.page.TablePage;
import com.database.db.core.table.Table;

public class PrimaryKey<K extends Comparable<? super K>> extends IndexInit<K> {
    public PrimaryKey(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.columnIndex = columnIndex;
    }
}
