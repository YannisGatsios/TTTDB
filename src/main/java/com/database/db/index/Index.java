package com.database.db.index;


import com.database.db.page.Page;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class Index<K extends Comparable<? super K>> extends BTreeSerialization<K> {
    
    public Index(Table table, int columnIndex) {
        super(Page.getPageCapacity(TablePage.sizeOfEntry(table)));
        this.setUnique(false);
        this.setNullable(true);
        this.columnIndex = columnIndex;
    }
}
