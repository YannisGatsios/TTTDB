package com.database.db.api;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;

import com.database.db.api.Schema.ColumnInner;
import com.database.db.table.Constraint;
import com.database.db.table.DataType;

public class Column {
    private Schema schema;

    private String name;
    private DataType type;
    private int size;
    private Set<Constraint> constraints = EnumSet.noneOf(Constraint.class);
    private Object defaultValue;
    
    public Column(){}
    public Column(String name, Schema schema){
        this.schema = schema;
        this.name = name;
    }

    public Column name(String name){
        this.name = name;
        return this;
    }
    public Column type(DataType type){
        this.type = type;
        if(this.type.getSize() != -1) this.size = this.type.getSize();
        return this;
    }
    public Column size(int size){
        if(this.type.getSize() == -1) this.size = size;
        else this.size = this.type.getSize();
        return this;
    }
    public Column primaryKey(){
        this.constraints.add(Constraint.PRIMARY_KEY);
        this.constraints.add(Constraint.NOT_NULL);
        return this;
    }
    public Column unique(){
        this.constraints.add(Constraint.UNIQUE);
        return this;
    }
    public Column index(){
        this.constraints.add(Constraint.INDEX);
        return this;
    }
    public Column autoIncrementing(){
        this.type(DataType.LONG);
        this.constraints.add(Constraint.AUTO_INCREMENT);
        return this;
    }
    public Column notNull(){
        this.constraints.add(Constraint.NOT_NULL);
        return this;
    }
    public Column check(String expresion){
        this.constraints.add(Constraint.CHECK);
        return this;
    }
    public Column defaultValue(Object value) {
        if (this.type == null) 
            throw new IllegalStateException("Column type must be set before assigning a default value.");
        if (!this.type.isValid(value)) 
            throw new IllegalArgumentException(
                "Invalid type for default value. Expected " 
                + type.getJavaClass().getSimpleName() 
                + " but got " + value.getClass().getSimpleName()
            );
        this.defaultValue = value;
        return this;
    }
    public Schema endColumn(){
        this.schema.add(get());
        return this.schema;
    }
    public ColumnInner get(){
        if(this.constraints.size()==0) this.constraints.add(Constraint.NO_CONSTRAINT);
        return new ColumnInner(
            name, 
            type, 
            size, 
            new ArrayList<>(constraints), 
            defaultValue
        );
    }
}