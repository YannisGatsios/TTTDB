package com.database.db.manager;

import java.util.ArrayList;
import java.util.List;

import com.database.db.index.BTreeInit.BlockPointer;
import com.database.db.index.BTreeInit.PointerPair;
import com.database.db.index.Pair;
import com.database.db.page.Entry;
import com.database.db.page.TablePage;
import com.database.db.table.Table;

public class SequentialOperations {
    public static long getMaxSequential(Table table, int columnIndex){
        long max = 0;
        for (int i = 0;i<table.getPages();i++){
            TablePage page = table.getCache().getTablePage(i);
            Entry[] entries = page.getAll();
            for (Entry entry : entries) {
                if(max < (long)entry.get(columnIndex)) max = (long)entry.get(columnIndex);
            }
        }
        return max;
    }
        @SuppressWarnings("unchecked")
    public static <K extends Comparable<? super K>> List<Pair<K,PointerPair>> sequentialRangeSearch(Table table, K upper, K lower, int columnIndex){
        List<Pair<K,PointerPair>> result = new ArrayList<>();
        for(int i = 0; i < table.getPages(); i++){
            TablePage page = table.getCache().getTablePage(i);
            for(int y = 0; y < page.size(); y++){
                Entry entry = page.get(y);
                K value = (K) entry.get(columnIndex);

                // Filter by range
                if((lower == null || value.compareTo(lower) >= 0) && (upper == null || value.compareTo(upper) <= 0)){
                    PointerPair pointer = new PointerPair(new BlockPointer(i, (short)y), null);
                    result.add(new Pair<>(value, pointer));
                }
            }
        }
        return result;
    }
}
