package com.database.db.index;


import com.database.db.page.Page;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class PrimaryKey<K extends Comparable<? super K>> extends BTreeInit<K> {
    public PrimaryKey(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.columnIndex = columnIndex;
    }
}
