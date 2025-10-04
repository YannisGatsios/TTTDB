package com.database.db.index;

import com.database.db.page.Page;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class Unique<K extends Comparable<? super K>> extends BTreeInit<K> {
    public Unique(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.setNullable(true);
        this.columnIndex = columnIndex;
    }
}
