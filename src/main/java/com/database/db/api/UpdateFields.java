package com.database.db.api;

import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Functions.*;

public class UpdateFields {
    private List<Map.Entry<String,InnerFunctions>> functionsList;
    private String column;
    public UpdateFields(String column){
        this.column = column;
        this.functionsList = new ArrayList<>();
    }

    public UpdateFields selectColumn(String column){
        this.column = column;
        return this;
    }

    private void addFunction(String column, InnerFunctions function){
        Map.Entry<String, InnerFunctions> entry = new AbstractMap.SimpleEntry<>(column, function);
        this.functionsList.add(entry);
    }

    public UpdateFields set(Object value){
        InnerFunctions function = new setData(value);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields operation( String expression){
        InnerFunctions function = new operationData(expression);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields currentTimestamp(){
        InnerFunctions function = new currentTimestamp();
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields dateAdd( String unit, long amount){
        InnerFunctions function = new dateAdd(unit, amount);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields dateSubtract( String unit, long amount){
        InnerFunctions function = new dateSub(unit, amount);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields extractPart( String part){
        InnerFunctions function = new extractPart(part);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields formatDate( String pattern){
        InnerFunctions function = new formatDate(pattern);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields dateDifference( LocalDateTime from, LocalDateTime to, String unit){
        InnerFunctions function = new dateDifference(from, to, unit);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields concat( Object[] parts){
        InnerFunctions function = new concat(parts);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields upperCase(){
        InnerFunctions function = new upperCase();
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields lowerCase(){
        InnerFunctions function = new lowerCase();
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields trim(){
        InnerFunctions function =  new trim();
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields substring( int start, int length){
        InnerFunctions function =  new substring(start,length);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields left( int length){
        InnerFunctions function = new left(length);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields right( int length){
        InnerFunctions function = new right(length);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields replace( String target, String replacement){
        InnerFunctions function = new replace(target, replacement);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields regexpReplace( String regex, String replacement){
        InnerFunctions function = new regexpReplace(regex, replacement);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields leftPad( int totalWidth, String padStr){
        InnerFunctions function = new leftPad(totalWidth, padStr);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields rightPad(int totalWidth, String padStr){
        InnerFunctions function = new rightPad(totalWidth, padStr);
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields reverse(){
        InnerFunctions function = new reverse();
        this.addFunction(column, function);
        return this;
    }
    public UpdateFields conditional(UpdateCondition conditionalUpdate, UpdateFields updateFields){
        conditionalUpdate.setUpdateFields(updateFields);
        this.addFunction(updateFields.getColumn(), conditionalUpdate);
        return this;
    }
    /**
    * This method is used internally 
    */
    public List<Map.Entry<String, InnerFunctions>> getFunctionsList() {
        return functionsList;
    }
    /**
    * This method is used internally 
    */
    public String getColumn(){
        return this.column;
    }
}
