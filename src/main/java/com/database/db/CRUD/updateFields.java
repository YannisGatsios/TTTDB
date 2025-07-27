package com.database.db.CRUD;

import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.database.db.CRUD.Functions.*;

public class updateFields {
    private List<Map.Entry<String,InnerFunctions>> functionsList;
    public updateFields(){
        this.functionsList = new ArrayList<>();
    }

    private void addFunction(String column, InnerFunctions function){
        Map.Entry<String, InnerFunctions> entry = new AbstractMap.SimpleEntry<>(column, function);
        this.functionsList.add(entry);
    }

    public updateFields set(String column,Object value){
        InnerFunctions function = new setData(value);
        this.addFunction(column, function);
        return this;
    }
    public updateFields operation(String column, String expression){
        InnerFunctions function = new operationData(expression);
        this.addFunction(column, function);
        return this;
    }
    public updateFields currentTimestamp(String column){
        InnerFunctions function = new currentTimestamp();
        this.addFunction(column, function);
        return this;
    }
    public updateFields dateAdd(String column, String unit, long amount){
        InnerFunctions function = new dateAdd(unit, amount);
        this.addFunction(column, function);
        return this;
    }
    public updateFields dataSub(String column, String unit, long amount){
        InnerFunctions function = new dateSub(unit, amount);
        this.addFunction(column, function);
        return this;
    }
    public updateFields extractPart(String column, String part){
        InnerFunctions function = new extractPart(part);
        this.addFunction(column, function);
        return this;
    }
    public updateFields formatDate(String column, String pattern){
        InnerFunctions function = new formatDate(pattern);
        this.addFunction(column, function);
        return this;
    }
    public updateFields dateDifference(String column, LocalDateTime from, LocalDateTime to, String unit){
        InnerFunctions function = new dateDifference(from, to, unit);
        this.addFunction(column, function);
        return this;
    }
    public updateFields concat(String column, Object[] parts){
        InnerFunctions function = new concat(parts);
        this.addFunction(column, function);
        return this;
    }
    public updateFields upperCase(String column){
        InnerFunctions function = new upperCase();
        this.addFunction(column, function);
        return this;
    }
    public updateFields lowerCase(String column){
        InnerFunctions function = new lowerCase();
        this.addFunction(column, function);
        return this;
    }
    public updateFields trim(String column){
        InnerFunctions function =  new trim();
        this.addFunction(column, function);
        return this;
    }
    public updateFields substring(String column, int start, int length){
        InnerFunctions function =  new substring(start,length);
        this.addFunction(column, function);
        return this;
    }
    public updateFields left(String column, int length){
        InnerFunctions function = new left(length);
        this.addFunction(column, function);
        return this;
    }
    public updateFields right(String column, int length){
        InnerFunctions function = new right(length);
        this.addFunction(column, function);
        return this;
    }
    public updateFields replace(String column, String target, String replacement){
        InnerFunctions function = new replace(target, replacement);
        this.addFunction(column, function);
        return this;
    }
    public updateFields regexpReplace(String column, String regex, String replacement){
        InnerFunctions function = new regexpReplace(regex, replacement);
        this.addFunction(column, function);
        return this;
    }
    public updateFields leftPad(String column, int totalWidth, String padStr){
        InnerFunctions function = new leftPad(totalWidth, padStr);
        this.addFunction(column, function);
        return this;
    }
    public updateFields rightPad(String column,int totalWidth, String padStr){
        InnerFunctions function = new rightPad(totalWidth, padStr);
        this.addFunction(column, function);
        return this;
    }
    public updateFields reverse(String column){
        InnerFunctions function = new reverse();
        this.addFunction(column, function);
        return this;
    }
    public updateFields ifNull(String column, Object fallback){
        InnerFunctions function = new ifNull(fallback);
        this.addFunction(column, function);
        return this;
    }
    public updateFields ifNotNull(String column, Object fallback){
        InnerFunctions function = new ifNotNull(fallback);
        this.addFunction(column, function);
        return this;
    }

    public List<Map.Entry<String, InnerFunctions>> getFunctionsList() {
        return functionsList;
    }
}
