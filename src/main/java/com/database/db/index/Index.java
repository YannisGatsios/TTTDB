package com.database.db.index;

import java.util.concurrent.ExecutionException;

import com.database.db.FileIO;
import com.database.db.table.Table;

public class Index<K extends Comparable<? super K>> extends BTreeSerialization<K> {
    
    public Index(Table table, int columnIndex) throws InterruptedException, ExecutionException{
        super(table.getPageCapacity());
        this.setUnique(false);
        this.setNullable(true);
        this.columnIndex = columnIndex;
        FileIO fileIO = new FileIO(table.getFileIOThread());
        byte[] treeBuffer = fileIO.readTree(table.getIndexPath(this.columnIndex));
        if (treeBuffer == null || treeBuffer.length == 0)
            return;
        this.fromBytes(treeBuffer, table);
    }
}
