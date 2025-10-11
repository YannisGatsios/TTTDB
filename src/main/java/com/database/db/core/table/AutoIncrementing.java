package com.database.db.core.table;

import com.database.db.core.manager.SequentialOperations;

public class AutoIncrementing {
    private long nextKey;

    public AutoIncrementing(long startingKey) {
        this.nextKey = startingKey;
    }

    // Call this to get the next key for an insert
    public long getNextKey() {
        return this.nextKey++;
    }

    public long getKey(){return this.nextKey;}

    // You would initialize this generator by finding the max key currently in the
    // table.
    public void setNextKey(long key) {
        this.nextKey = key+1;
    }

    public static AutoIncrementing[] prepareAutoIncrementing(Table table){
        AutoIncrementing[] result = new AutoIncrementing[table.getSchema().getNumOfColumns()];
        boolean[] isAutoIncrementing = table.getSchema().getAutoIncrementIndex();
        for (int i = 0;i<result.length;i++){
            if(isAutoIncrementing[i]){
                long max;
                Object maxValue = table.getIndexManager().getMax(i);
                if(table.getIndexManager().isIndexed(i) && maxValue != null) max = (long)maxValue;
                else if (table.getIndexManager().isIndexed(i) && maxValue == null) max = 0;
                else{
                    max = SequentialOperations.getMaxSequential(table, i);
                }
                result[i] = new AutoIncrementing(max+1);
            }
        }
        return result;
    }
}
