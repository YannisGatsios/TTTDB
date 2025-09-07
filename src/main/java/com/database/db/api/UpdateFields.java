package com.database.db.api;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Functions.*;

public class UpdateFields {
    private List<InnerFunctions> functionsList;
    public UpdateFields(String column){
        this.functionsList = new ArrayList<>();
        this.functionsList.add(new selectColumn(column));
    }

    public UpdateFields selectColumn(String column){
        this.functionsList.add(new selectColumn(column));
        return this;
    }

    public UpdateFields set(Object value){
        InnerFunctions function = new setData(value);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields operation( String expression){
        InnerFunctions function = new operationData(expression);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields currentTimestamp(){
        InnerFunctions function = new currentTimestamp();
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields dateAdd( String unit, long amount){
        InnerFunctions function = new dateAdd(unit, amount);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields dateSubtract( String unit, long amount){
        InnerFunctions function = new dateSub(unit, amount);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields extractPart( String part){
        InnerFunctions function = new extractPart(part);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields formatDate( String pattern){
        InnerFunctions function = new formatDate(pattern);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields dateDifference( LocalDateTime from, LocalDateTime to, String unit){
        InnerFunctions function = new dateDifference(from, to, unit);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields concat( Object[] parts){
        InnerFunctions function = new concat(parts);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields upperCase(){
        InnerFunctions function = new upperCase();
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields lowerCase(){
        InnerFunctions function = new lowerCase();
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields trim(){
        InnerFunctions function =  new trim();
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields substring( int start, int length){
        InnerFunctions function =  new substring(start,length);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields left( int length){
        InnerFunctions function = new left(length);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields right( int length){
        InnerFunctions function = new right(length);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields replace( String target, String replacement){
        InnerFunctions function = new replace(target, replacement);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields regexpReplace( String regex, String replacement){
        InnerFunctions function = new regexpReplace(regex, replacement);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields leftPad( int totalWidth, String padStr){
        InnerFunctions function = new leftPad(totalWidth, padStr);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields rightPad(int totalWidth, String padStr){
        InnerFunctions function = new rightPad(totalWidth, padStr);
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields reverse(){
        InnerFunctions function = new reverse();
        this.functionsList.add(function);
        return this;
    }
    public UpdateFields condition(String column){
        UpdateCondition updateCondition = new UpdateCondition(this);
        this.functionsList.add(updateCondition);
        return this;
    }
    public UpdateFields endConditionalUpdate(){
        this.functionsList.add(new endConditionalUpdate());
        return this;
    }
    /**
    * This method is used internally 
    */
    public List<InnerFunctions> getFunctionsList() {
        return functionsList;
    }
    /**
    * This method is used internally 
    */
}