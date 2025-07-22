package com.database.db.table;

public class AutoIncrementing {
    private long nextKey;

    public AutoIncrementing(long startingKey) {
        this.nextKey = startingKey;
    }

    // Call this to get the next key for an insert
    public long getNextKey() {
        return this.nextKey++;
    }

    // You would initialize this generator by finding the max key currently in the
    // table.
    public void setNextKey(long key) {
        this.nextKey = key;
    }
}
