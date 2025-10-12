package com.database.tttdb.api;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.database.tttdb.api.Functions.operationData;
import com.database.tttdb.api.Query.Delete;
import com.database.tttdb.api.Query.Select;
import com.database.tttdb.api.Query.Update;
import com.database.tttdb.core.table.TableSchema;
/**
 * Represents a single condition in a {@link ConditionGroup}, such as a {@link WhereClause}.
 * 
 * <p>Conditions are used to filter rows based on column values or custom expressions.
 * This class provides a fluent API to define comparisons, null checks, and expressions.
 * Generic parameter {@code T} ensures that chaining returns the proper {@link ConditionGroup} type.</p>
 * 
 * <p>Example usage:</p>
 * <pre>
 * // Simple column comparison in a WHERE clause
 * WhereClause where = new WhereClause();
 * where.column("age").isBiggerOrEqual(18).end()
 *      .AND()
 *      .column("age").isSmaller(130).end();
 *
 * // Expression-based condition (not allowed in WhereClause)
 * ConditionGroup group = new ConditionGroup();
 * group.condition().expression("age + 5 > 20").end();
 * </pre>
 * 
 * @param <T> the concrete type of the condition group, used for fluent chaining
 */
public class Condition<T extends ConditionGroup<T>> {
    private String columnName;
    private operationData expression;
    private final EnumMap<Conditions, Object> conditionElementsList = new EnumMap<>(Conditions.class);
    private final T group;

    /**
     * Constructs a condition attached to the given group.
     * 
     * @param group the condition group this condition belongs to
     */
    public Condition(T group) { this.group = group; }

    /**
     * Sets the column this condition applies to.
     * 
     * <p>Cannot be combined with {@link #expression(String)} in the same condition.</p>
     * 
     * @param column the column name
     * @return the current {@code Condition} instance for fluent chaining
     */
    public Condition<T> column(String column) {
        if (this.expression != null) throw new IllegalArgumentException("Can't set column when expression is set");
        this.columnName = column;
        return this;
    }
    /**
     * Sets a custom expression for this condition.
     * 
     * <p>Expressions are not allowed in {@link WhereClause}; only column-based conditions are supported there.</p>
     * 
     * @param expr the expression string
     * @return the current {@code Condition} instance for fluent chaining
     * @throws UnsupportedOperationException if used inside a {@link WhereClause}
     */
    public Condition<T> expression(String expr) {
        // Enforce flat-only restriction: expressions are not allowed in WhereClause
        if (group instanceof WhereClause) 
            throw new UnsupportedOperationException("Expressions are not supported in WhereClause. Use column() with simple conditions instead.");
        if (this.columnName != null) throw new IllegalArgumentException("Can't set expression when column is set");
        this.expression = new operationData(expr);
        return this;
    }

    // comparison builders
    /** Checks if column value is null. */
    public Condition<T> isNull(){ conditionElementsList.put(Conditions.IS_EQUAL, null); return this; }
    /** Checks if column value is not null. */
    public Condition<T> notNull(){ conditionElementsList.put(Conditions.IS_NOT_EQUAL, null); return this; }
    /** Checks if column value equals the given value. */
    public Condition<T> isEqual(Object v){ conditionElementsList.put(Conditions.IS_EQUAL, v); return this; }
    /** Checks if column value does not equal the given value. */
    public Condition<T> isNotEqual(Object v){ conditionElementsList.put(Conditions.IS_NOT_EQUAL, v); return this; }
    /** Checks if column value is greater than the given value. */
    public Condition<T> isBigger(Object v){ conditionElementsList.put(Conditions.IS_BIGGER, v); return this; }
    /** Checks if column value is smaller than the given value. */
    public Condition<T> isSmaller(Object v){ conditionElementsList.put(Conditions.IS_SMALLER, v); return this; }
    /** Checks if column value is greater than or equal to the given value. */
    public Condition<T> isBiggerOrEqual(Object v){ conditionElementsList.put(Conditions.IS_BIGGER_OR_EQUAL, v); return this; }
    /** Checks if column value is smaller than or equal to the given value. */
    public Condition<T> isSmallerOrEqual(Object v){ conditionElementsList.put(Conditions.IS_SMALLER_OR_EQUAL, v); return this; }

    /**
     * Ends the condition definition and returns the parent condition group.
     * 
     * @return the parent {@code ConditionGroup} for continued fluent chaining
     */
    public T end() { return this.group; }

    /**
     * Evaluates this condition against a single value.
     * 
     * @param value the value to test
     * @return {@code true} if the value satisfies all defined conditions
     */
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
    /**
     * Evaluates this condition against a row of values using the schema.
     * 
     * @param entryValues the values of a row
     * @param schema the schema defining column indices
     * @return {@code true} if the condition is satisfied
     */
    public boolean isTrue(Object[] entryValues, TableSchema schema) {
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
    /**
     * Validates this condition against a schema.
     * 
     * <p>If a column is used, it must exist in the schema. If an expression
     * is used, all referenced columns must exist.</p>
     * 
     * @param schema the schema to validate against
     * @return {@code true} if the condition is valid
     */
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
    /**
     * Defines how conditions are logically connected in a clause.
     */
    public static enum Clause{
        FIRST,
        OR,
        AND,
        FIRST_GROUP,
        OR_GROUP,
        AND_GROUP;
        /**
         * Defines the supported comparison operations for a condition.
         */
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
    /**
     * Represents a root-level WHERE clause for filtering rows.
     * 
     * <p>This is a flat-only implementation of {@link ConditionGroup}. Nested groups
     * and expressions are not supported. Only simple column-based conditions
     * can be defined.</p>
     * 
     * <p>Example usage:</p>
     * <pre>
     * WhereClause where = new WhereClause();
     * where.column("age").isBiggerOrEqual(18).end()
     *      .AND()
     *      .column("age").isSmaller(130).end();
     * </pre>
     */
    public static class WhereClause extends ConditionGroup<WhereClause> {
        private Query query;
        /** Root constructor (no parent). Used for creating the main WhereClause. */
        public WhereClause() {
            super(null);
        }
        public WhereClause(Query query){
            super(null);
            this.query = query;
        }
        /** Child constructor: allows proper parent reference for potential close() calls. */
        protected WhereClause(WhereClause parent) {
            super(parent);
        }
        public Select endSelectClause(){
            this.query.set(this);
            return (Select) this.query;
        }
        public Delete endDeleteClause(){
            this.query.set(this);
            return (Delete) this.query;
        }
        public Update endUpdateClause(){
            this.query.set(this);
            return (Update) this.query;
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
        public Condition<WhereClause> expression(String expression) {
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
                public Condition<WhereClause> expression(String expression) {
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
                public Condition<WhereClause> expression(String expression) {
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
    /**
     * Represents a conditional update used in an UPDATE statement.
     * 
     * <p>This class extends {@link ConditionGroup} to allow defining conditions
     * that determine which rows will be updated. It links back to the 
     * {@link UpdateFields} instance that initiated the conditional update,
     * enabling fluent continuation of the API.</p>
     * 
     * <p>Example usage:</p>
     * <pre>
     * updateFields.condition()
     *     .column("age").isBigger(18).end()
     *     .endConditionalUpdate()
     *     .set("status", "adult");
     * </pre>
     */
    public static class UpdateCondition extends ConditionGroup<UpdateCondition> implements Functions.InnerFunctions {
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
         * Ends the conditional update and returns the {@link UpdateFields} object
         * to continue the fluent update API.
         * 
         * @return the original {@link UpdateFields} instance
         */
        public UpdateFields endConditionalUpdate() {
            return this.updateFields.endConditionalUpdate();
        }
        /**
         * Return this group so callers can build the condition: updateField.condition(...).column(...).isBigger(10).end() ...
         */
        public ConditionGroup<UpdateCondition> getCondition() {
            return this;
        }
        /**
         * Evaluates the condition against a row of data.
         * 
         * @param schema the schema defining column indices
         * @param rowData the values of the row to evaluate
         * @return true if all conditions in this group are satisfied
         */
        public boolean isTrue(TableSchema schema, Object[] rowData) {
            return super.isTrue(rowData, schema);
        }
        /**
         * This object is a control function (it doesn't itself compute a value when applied).
         * If you do want it to produce a value (e.g. evaluate the condition and return something),
         * implement it here. For now we keep it as unsupported to avoid accidental use.
         */
        @Override
        public Object apply(TableSchema schema, Object[] rowData, int columnIndex) {
            throw new UnsupportedOperationException("UpdateCondition is a control function; it cannot be applied as a value.");
        }
    }
    /**
     * Represents a condition group used for CHECK constraints on a table schema.
     * 
     * <p>Allows defining complex conditions for a CHECK constraint, optionally
     * nested with child groups.</p>
     * 
     * <p>Example usage:</p>
     * <pre>
     * schema.check("age_check")
     *       .open()
     *          .column("num").isBiggerOrEqual(18).end()
     *       .close()
     *       .AND()
     *       .column("num").isSmaller(130).end()
     *       .endCheck();
     * </pre>
     */
    public static class CheckCondition extends ConditionGroup<CheckCondition> {
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
        /**
         * Ends the CHECK condition definition and returns the parent schema.
         * 
         * @return the {@link Schema} that owns this check
         */
        public Schema endCheck(){
            return this.check.endCheck();
        }
    }
}