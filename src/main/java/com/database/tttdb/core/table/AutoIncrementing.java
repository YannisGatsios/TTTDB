package com.database.tttdb.core.table;

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
                Object maxValue = table.getIndexManager().getMax(i);
                long max = maxValue==null? 0:(long)maxValue;
                result[i] = new AutoIncrementing(max+1);
            }
        }
        return result;
    }
}
