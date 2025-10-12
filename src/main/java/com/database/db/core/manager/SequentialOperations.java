package com.database.db.core.manager;

import java.util.ArrayList;
import java.util.List;

import com.database.db.core.index.Pair;
import com.database.db.core.index.IndexInit.BlockPointer;
import com.database.db.core.index.IndexInit.PointerPair;
import com.database.db.core.page.Entry;
import com.database.db.core.page.TablePage;
import com.database.db.core.table.Table;

public class SequentialOperations {
    @SuppressWarnings("unchecked")
        public static <K extends Comparable<? super K>> K getMaxSequential(Table table, int columnIndex) {
            if (table.getPages() == 0) return null;
            K max = null;
            final boolean skipNulls = !table.getSchema().getNotNull()[columnIndex]; // true if column can be null
            for (int pid = 0; pid < table.getPages(); pid++) {
                TablePage page = table.getCache().getTablePage(pid);
                int sz = page.size();
                for (int row = 0; row < sz; row++) {
                    Object v = page.get(row).get(columnIndex);
                    if (skipNulls && v == null) continue;
                    K val = (K) v;
                    if (val == null) continue;           // safety if column declared NOT NULL but value is null
                    if (max == null || val.compareTo(max) > 0) max = val;
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
