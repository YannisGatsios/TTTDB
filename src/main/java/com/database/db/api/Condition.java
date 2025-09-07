package com.database.db.api;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.database.db.api.Functions.operationData;
import com.database.db.table.SchemaInner;

public class Condition {
    public static enum Clause{
        FIRST,
        OR,
        AND,
        FIRST_GROUP,
        OR_GROUP,
        AND_GROUP;

        public boolean isGroup() {
            return this == FIRST_GROUP || this == OR_GROUP || this == AND_GROUP;
        }
    }
    public enum Conditions{
        IS_BIGGER,
        IS_SMALLER,
        IS_EQUAL,
        IS_NOT_EQUAL,
        IS_SMALLER_OR_EQUAL,
        IS_BIGGER_OR_EQUAL,
    }
    public static class Cond<T extends CondGroup<T>> {
        private String columnName;
        private operationData expression;
        private final EnumMap<Conditions, Object> conditionElementsList = new EnumMap<>(Conditions.class);
        private final T group;

        public Cond(T group) { this.group = group; }

        public Cond<T> column(String column) {
            if (this.expression != null) throw new IllegalArgumentException("Can't set column when expression is set");
            this.columnName = column;
            return this;
        }
        public Cond<T> expression(String expr) {
            // Enforce flat-only restriction: expressions are not allowed in WhereClause
            if (group instanceof WhereClause) 
                throw new UnsupportedOperationException("Expressions are not supported in WhereClause. Use column() with simple conditions instead.");
            if (this.columnName != null) throw new IllegalArgumentException("Can't set expression when column is set");
            this.expression = new operationData(expr);
            return this;
        }

        // comparison builders
        public Cond<T> isNull(){ conditionElementsList.put(Conditions.IS_EQUAL, null); return this; }
        public Cond<T> notNull(){ conditionElementsList.put(Conditions.IS_NOT_EQUAL, null); return this; }
        public Cond<T> isEqual(Object v){ conditionElementsList.put(Conditions.IS_EQUAL, v); return this; }
        public Cond<T> isNotEqual(Object v){ conditionElementsList.put(Conditions.IS_NOT_EQUAL, v); return this; }
        public Cond<T> isBigger(Object v){ conditionElementsList.put(Conditions.IS_BIGGER, v); return this; }
        public Cond<T> isSmaller(Object v){ conditionElementsList.put(Conditions.IS_SMALLER, v); return this; }
        public Cond<T> isBiggerOrEqual(Object v){ conditionElementsList.put(Conditions.IS_BIGGER_OR_EQUAL, v); return this; }
        public Cond<T> isSmallerOrEqual(Object v){ conditionElementsList.put(Conditions.IS_SMALLER_OR_EQUAL, v); return this; }

        // return the concrete group type so chaining keeps the specific type
        public T end() { return this.group; }

        // evaluation helpers (same as your logic)
        @SuppressWarnings("unchecked")
        public boolean isApplicable(Object value) {
            for (Map.Entry<Conditions, Object> cond : conditionElementsList.entrySet()) {
                boolean passed = switch (cond.getKey()) {
                    case IS_EQUAL -> Objects.equals(value, cond.getValue());
                    case IS_NOT_EQUAL -> !Objects.equals(value, cond.getValue());
                    case IS_BIGGER -> {
                        if (value instanceof Comparable<?> && cond.getValue() instanceof Comparable<?>)
                            yield ((Comparable<Object>) value).compareTo(cond.getValue()) > 0;
                        yield false;
                    }
                    case IS_SMALLER -> {
                        if (value instanceof Comparable<?> && cond.getValue() instanceof Comparable<?>)
                            yield ((Comparable<Object>) value).compareTo(cond.getValue()) < 0;
                        yield false;
                    }
                    case IS_BIGGER_OR_EQUAL -> {
                        if (value instanceof Comparable<?> && cond.getValue() instanceof Comparable<?>)
                            yield ((Comparable<Object>) value).compareTo(cond.getValue()) >= 0;
                        yield false;
                    }
                    case IS_SMALLER_OR_EQUAL -> {
                        if (value instanceof Comparable<?> && cond.getValue() instanceof Comparable<?>)
                            yield ((Comparable<Object>) value).compareTo(cond.getValue()) <= 0;
                        yield false;
                    }
                };
                if (!passed) return false;
            }
            return true;
        }

        public boolean isTrue(Object[] entryValues, SchemaInner schema) {
            Object value;
            if (columnName != null) {
                int idx = schema.getColumnIndex(columnName);
                value = entryValues[idx];
            } else if (expression != null) {
                value = this.expression.apply(schema, entryValues, -1);
            } else {
                throw new IllegalStateException("Cond must have column or expression");
            }
            if (value == null) return false;
            return isApplicable(value);
        }

        public boolean isValid(Schema schema) {
            if (columnName != null) return schema.hasColumn(columnName);
            if (expression != null) {
                Pattern p = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
                Matcher m = p.matcher(expression.expression());
                while (m.find()) {
                    if (!schema.hasColumn(m.group())) return false;
                }
                return true;
            }
            return false;
        }

        public String getColumnName() { return this.columnName; }
        public EnumMap<Conditions, Object> getConditions() { return this.conditionElementsList; }
    }
    public static class WhereClause extends CondGroup<WhereClause> {
        /** Root constructor (no parent). Used for creating the main WhereClause. */
        public WhereClause() {
            super(null);
        }
        /** Child constructor: allows proper parent reference for potential close() calls. */
        protected WhereClause(WhereClause parent) {
            super(parent);
        }
        /** 
         * Factory for child groups. 
         * Returns a new WhereClause instance, but nested groups are not supported here.
         */
        @Override
        protected WhereClause newGroup() {
            return new WhereClause(this);
        }
        /** 
         * Expressions are not supported in WhereClause.
         * Only flat column-based conditions are allowed.
         */
        @Override
        public Condition.Cond<WhereClause> expression(String expression) {
            throw new UnsupportedOperationException(
                "Expressions are not supported in WhereClause. " +
                "Use column() with simple conditions instead."
            );
        }
        /** 
         * Flat OR operator for WhereClause.
         * Nested groups via OR.open() and expressions via OR.expression() are disabled.
         */
        @Override
        public OR OR() {
            return new OR(this) {
                @Override
                public WhereClause open() {
                    throw new UnsupportedOperationException(
                        "Nested OR groups (open()) are not supported in WhereClause. " +
                        "Use only flat OR conditions via column()."
                    );
                }

                @Override
                public Condition.Cond<WhereClause> expression(String expression) {
                    throw new UnsupportedOperationException(
                        "Expressions are not supported in OR within WhereClause. " +
                        "Use column() with simple conditions instead."
                    );
                }
            };
        }
        /** 
         * Flat AND operator for WhereClause.
         * Nested groups via AND.open() and expressions via AND.expression() are disabled.
         */
        @Override
        public AND AND() {
            return new AND(this) {
                @Override
                public WhereClause open() {
                    throw new UnsupportedOperationException(
                        "Nested AND groups (open()) are not supported in WhereClause. " +
                        "Use only flat AND conditions via column()."
                    );
                }

                @Override
                public Condition.Cond<WhereClause> expression(String expression) {
                    throw new UnsupportedOperationException(
                        "Expressions are not supported in AND within WhereClause. " +
                        "Use column() with simple conditions instead."
                    );
                }
            };
        }
        /** 
         * open() is disabled at the root level since WhereClause is flat-only.
         */
        @Override
        public WhereClause open() {
            throw new UnsupportedOperationException(
                "Nested groups (open()) are not supported in WhereClause. " +
                "Define only flat column() conditions."
            );
        }
        /** 
         * close() is disabled at the root level since WhereClause cannot have nested groups.
         */
        @Override
        public WhereClause close() {
            throw new UnsupportedOperationException(
                "close() is not supported in WhereClause. " +
                "Nested groups are not allowed, so there is nothing to close."
            );
        }
    }

    public static class UpdateCondition extends CondGroup<UpdateCondition> implements Functions.InnerFunctions {
        private final UpdateFields updateFields;
        // Root constructor (parent == null)
        public UpdateCondition(UpdateFields updateFields) {
            super(null);
            this.updateFields = updateFields;
        }
        // Child constructor (set parent)
        protected UpdateCondition(UpdateFields updateFields, UpdateCondition parent) {
            super(parent);
            this.updateFields = updateFields;
        }
        @Override
        protected UpdateCondition newGroup() {
            // create a child and set its parent via constructor
            return new UpdateCondition(this.updateFields, this);
        }
        /**
         * Return the UpdateFields that started this conditional update (so the fluent API can continue).
         */
        public UpdateFields endConditionalUpdate() {
            return this.updateFields.endConditionalUpdate();
        }
        /**
         * Return this group so callers can build the condition: updateField.condition(...).column(...).isBigger(10).end() ...
         */
        public CondGroup<UpdateCondition> getCondition() {
            return this;
        }
        /**
         * Helper for easier calling from outside code. Accepts parameters in order (table, rowData)
         * and delegates to the inherited condition evaluation.
         */
        public boolean isTrue(SchemaInner schema, Object[] rowData) {
            return super.isTrue(rowData, schema);
        }
        /**
         * This object is a control function (it doesn't itself compute a value when applied).
         * If you do want it to produce a value (e.g. evaluate the condition and return something),
         * implement it here. For now we keep it as unsupported to avoid accidental use.
         */
        @Override
        public Object apply(SchemaInner schema, Object[] rowData, int columnIndex) {
            throw new UnsupportedOperationException("UpdateCondition is a control function; it cannot be applied as a value.");
        }
    }
    public static class CheckCondition extends CondGroup<CheckCondition> {
        private final Check check;

        // root constructor (parent = null)
        public CheckCondition(Check check){
            super(null);
            this.check = check;
        }
        // child constructor (parent != null)
        public CheckCondition(Check check, CheckCondition parent){
            super(parent);
            this.check = check;
        }
        @Override
        protected CheckCondition newGroup() {
            // create a child group and correctly set its parent to 'this'
            return new CheckCondition(this.check, this);
        }
        public Schema endCheck(){
            return this.check.endCheck();
        }
    }
}