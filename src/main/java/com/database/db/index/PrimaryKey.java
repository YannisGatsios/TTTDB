package com.database.db.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import com.database.db.page.TablePage;
import com.database.db.table.Entry;
import com.database.db.table.Table;

public class PrimaryKey<K extends Comparable<? super K>> extends BPlusTree<K,Integer> {

    public PrimaryKey(int order){
        super(order);
    }

    public void initialize(Table<K> table) throws InterruptedException, ExecutionException, IOException {
        int numberOfPages = table.getPages();
        if(numberOfPages == 0) return;
        TablePage<K> page;
        ArrayList<Entry<K>> list;
        for(int i = 0;i <= numberOfPages;i++){
            page = new TablePage<>(i, table);
            list = page.getAll();
            for (Entry<K> entry : list) {
                this.insert(entry.getID(), i);
            }
        }
    }
}
