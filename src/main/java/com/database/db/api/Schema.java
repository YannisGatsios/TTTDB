package com.database.db.api;

import java.util.ArrayList;
import java.util.List;

import com.database.db.table.Constraint;
import com.database.db.table.DataType;

public class Schema {

    private List<ColumnInner> columns;

    public record ColumnInner(String name, DataType type, int size, List<Constraint> constraints, Object Default) {}
    
    public Schema(){
        this.columns = new ArrayList<>();
    }
    public void add(ColumnInner column){
        this.columns.add(column);
    }
    public Column column(String name){
        return new Column(name, this);
    }
    public ColumnInner[] get() {
        if (columns == null) {
            return new ColumnInner[0]; // return empty array instead of null
        }
        return columns.toArray(new ColumnInner[0]);
    }
}