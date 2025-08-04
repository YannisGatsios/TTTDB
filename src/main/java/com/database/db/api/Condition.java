package com.database.db.api;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.database.db.api.Functions.InnerFunctions;
import com.database.db.table.Schema;

public class Condition {
    public static enum Clause{
        AND,
        OR,
        FIRST,
    }
    public static class WhereClause{
        private String columnName;
        private EnumMap<Conditions, Object> conditionElementsList;
        private List<Map.Entry<Clause, WhereClause>> conditionsList;
        public WhereClause(String columnName){
            this.columnName = columnName;
            this.conditionElementsList = new EnumMap<>(Conditions.class);
            Map.Entry<Clause, WhereClause> first = new AbstractMap.SimpleEntry<>(Clause.FIRST,this);
            this.conditionsList = new ArrayList<>();
            this.conditionsList.add(first);
        }
        public WhereClause isNull(){
            this.conditionElementsList.put(Conditions.IS_EQUAL, null);
            return this;
        }
        public WhereClause notNull(){
            this.conditionElementsList.put(Conditions.IS_NOT_EQUAL, null);
            return this;
        }
        public WhereClause isEqual(Object value){
            this.conditionElementsList.put(Conditions.IS_EQUAL, value);
            return this;
        }
        public WhereClause isNotEqual(Object value){
            this.conditionElementsList.put(Conditions.IS_NOT_EQUAL, value);
            return this;
        }
        public WhereClause isBigger(Object value){
            this.conditionElementsList.put(Conditions.IS_BIGGER, value);
            return this;
        }
        public WhereClause isSmaller(Object value){
            this.conditionElementsList.put(Conditions.IS_SMALLER, value);
            return this;
        }
        public WhereClause OR(WhereClause clause){
            Map.Entry<Clause, WhereClause> newOrClause = new AbstractMap.SimpleEntry<>(Clause.OR,clause);
            this.conditionsList.add(newOrClause);
            return this;
        }
        public WhereClause AND(WhereClause clause){
            Map.Entry<Clause, WhereClause> newAndClause = new AbstractMap.SimpleEntry<>(Clause.OR,clause);
            this.conditionsList.add(newAndClause);
            return this;
        }
        public boolean isApplicable(Object value){
            for (Map.Entry<Conditions, Object> condition : this.conditionElementsList.entrySet()) {
                boolean passed = switch (condition.getKey()) {
                    case IS_EQUAL -> Objects.equals(value, condition.getValue());
                    case IS_NOT_EQUAL -> !Objects.equals(value, condition.getValue());
                    case IS_BIGGER -> {
                        if (value instanceof Comparable<?> && condition.getValue() instanceof Comparable<?>) {
                            yield ((Comparable<Object>) value).compareTo(condition.getValue()) > 0;
                        }
                        yield false;
                    }
                    case IS_SMALLER -> {
                        if (value instanceof Comparable<?> && condition.getValue() instanceof Comparable<?>) {
                            yield ((Comparable<Object>) value).compareTo(condition.getValue()) < 0;
                        }
                        yield false;
                    }
                };
                if (!passed) return false; // fail fast
            }
            return true;
        }
        public String getColumnName(){
            return this.columnName;
        }
        public EnumMap<Conditions, Object> getConditionElementsList(){
            return this.conditionElementsList;
        }
        public List<Map.Entry<Clause, WhereClause>> getClauseList(){
            return this.conditionsList;
        } 
    }
    public class UpdateCondition implements InnerFunctions{
        private List<Map.Entry<Conditions,Object>> conditionsList;
        private UpdateFields updateFields;
        public UpdateCondition(){
            this.conditionsList = new ArrayList<>();
        }
        public UpdateCondition isNull(){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_EQUAL, null);
            this.conditionsList.add(condition);
            return this;
        }
        public UpdateCondition notNull(){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_NOT_EQUAL, null);
            this.conditionsList.add(condition);
            return this;
        }
        public UpdateCondition isEqual(Object value){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_EQUAL, value);
            this.conditionsList.add(condition);
            return this;
        }
        public UpdateCondition isNotEqual(Object value){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_NOT_EQUAL, value);
            this.conditionsList.add(condition);
            return this;
        }
        public UpdateCondition isBigger(Object value){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_BIGGER, value);
            this.conditionsList.add(condition);
            return this;
        }
        public UpdateCondition isSmaller(Object value){
            Map.Entry<Conditions,Object> condition = new AbstractMap.SimpleEntry<>(Conditions.IS_SMALLER, value);
            this.conditionsList.add(condition);
            return this;
        }
        /**
        * This method is used internally
        */
        public List<Map.Entry<Conditions,Object>> getConditionList(){
            return this.conditionsList;
        }
        /**
        * This method is used internally 
        */
        public void setUpdateFields(UpdateFields updateFields){
            this.updateFields = updateFields;
        }
        /**
        * This method is used internally 
        */
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            Object value = data[columnIndex];
            if(!isApplicable(value)) return value;
            ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(schema.getNames()));
            int index = columnNames.indexOf(updateFields.getColumn());
            for (Map.Entry<String, InnerFunctions> update : this.updateFields.getFunctionsList()) {
                if (index < 0)
                    throw new IllegalArgumentException("Invalid column to update: " + update.getKey());
                value = update.getValue().apply(schema, data, index);
            }
            return value;
        }
        public boolean isApplicable(Object value){
            for (Map.Entry<Conditions, Object> condition : this.conditionsList) {
                boolean passed = switch (condition.getKey()) {
                    case IS_EQUAL -> Objects.equals(value, condition.getValue());
                    case IS_NOT_EQUAL -> !Objects.equals(value, condition.getValue());
                    case IS_BIGGER -> {
                        if (value instanceof Comparable<?> && condition.getValue() instanceof Comparable<?>) {
                            yield ((Comparable<Object>) value).compareTo(condition.getValue()) > 0;
                        }
                        yield false;
                    }
                    case IS_SMALLER -> {
                        if (value instanceof Comparable<?> && condition.getValue() instanceof Comparable<?>) {
                            yield ((Comparable<Object>) value).compareTo(condition.getValue()) < 0;
                        }
                        yield false;
                    }
                };
                if (!passed) return false; // fail fast
            }
            return true; // all passed
        }
    }
    public enum Conditions{
        IS_BIGGER,
        IS_SMALLER,
        IS_EQUAL,
        IS_NOT_EQUAL,
    }
}
