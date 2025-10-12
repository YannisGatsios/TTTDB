package com.database.db.api;

import com.database.db.api.Condition.CheckCondition;
import com.database.db.core.page.Entry;
import com.database.db.core.table.TableSchema;

public class Check {
    private final CheckCondition root;
    private final Schema schema;
    private final String name;
    public Check(String name, Schema schema) {
        this.name = name; this.schema = schema; this.root = new CheckCondition(this);
    }
    public CheckCondition open() { return this.root.open(); }      // returns CheckCondition
    public Condition<CheckCondition> column(String c) { return this.root.column(c); }
    public Condition<CheckCondition> expression(String c) { return this.root.column(c); }
    public Schema endCheck() { this.schema.add(this); return this.schema; }
    public boolean isTrue(Entry  entry, TableSchema schema) { return this.root.isTrue(entry.getValues(), schema); }
    public boolean isValid() { return this.root.isValid(schema); }
    public String name() { return this.name; }
}