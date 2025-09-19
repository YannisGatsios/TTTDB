package com.database.db.api;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.database.db.api.Condition.CheckCondition;
import com.database.db.api.Condition.Clause;
import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Condition.WhereClause;
import com.database.db.table.SchemaInner;

/**
 * Represents a group of conditions that can be combined using AND/OR logic.
 * 
 * <p>This is the base class for {@link WhereClause}, {@link UpdateCondition}, 
 * and {@link CheckCondition}. It manages a list of conditions and/or child 
 * groups, supporting nested condition structures.</p>
 * 
 * <p>Conditions can be either:</p>
 * <ul>
 *     <li>Column-based: directly referencing a column in the schema</li>
 *     <li>Expression-based: a formula or computation that evaluates to a value</li>
 * </ul>
 * 
 * <p>Child groups allow nesting of conditions, using <code>open()</code> and 
 * <code>close()</code>. Subclasses may restrict nesting (e.g., flat WhereClause).</p>
 * 
 * <p>Example usage:</p>
 * <pre>
 * ConditionGroup<?> group = new ConditionGroup<>();
 * group.column("age").isBiggerOrEqual(18).end();
 * group.AND().column("status").isEqual("active").end();
 * group.OR().column("role").isEqual("admin").end();
 * </pre>
 *
 * @param <T> the concrete subclass type of the condition group for fluent chaining
 */
public class ConditionGroup<T extends ConditionGroup<T>> {
    private final List<Map.Entry<Condition.Clause, Condition<T>>> clauses = new ArrayList<>();
    private final List<T> groups = new ArrayList<>();
    protected T parent;
    /** Default constructor, no parent group. */
    public ConditionGroup() {}
     /** Constructor with parent group reference for nested groups. */
    public ConditionGroup(T parent) { this.parent = parent; }

    /**
     * Adds a nested condition group to this group with a given clause type.
     * 
     * @param group the child condition group
     * @param clause the clause type (FIRST_GROUP, AND_GROUP, OR_GROUP)
     */
    protected void addGroup(T group, Condition.Clause clause) {
        this.clauses.add(new AbstractMap.SimpleEntry<>(clause, null));
        this.groups.add(group);
    }
    /**
     * Adds a single condition to this group with a given clause type.
     * 
     * @param condition the condition to add
     * @param clause the clause type (FIRST, AND, OR)
     */
    protected void addCondition(Condition<T> condition, Condition.Clause clause) {
        this.clauses.add(new AbstractMap.SimpleEntry<>(clause, condition));
    }

    /**
     * Starts defining a condition on a specific column.
     * Must be the first condition or follow an OR()/AND() call.
     * 
     * @param columnName the column name to filter on
     * @return a {@link Condition} builder for the column
     */
    public Condition<T> column(String columnName) {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before column()");
        @SuppressWarnings("unchecked")
        Condition<T> cond = new Condition<>((T) this);
        cond.column(columnName);
        clauses.add(new AbstractMap.SimpleEntry<>(Condition.Clause.FIRST, cond));
        return cond;
    }
    /**
     * Starts defining a condition using an expression.
     * Must be the first condition or follow an OR()/AND() call.
     * 
     * @param expression the expression string to evaluate
     * @return a {@link Condition} builder for the expression
     */
    public Condition<T> expression(String expression) {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before expression()");
        @SuppressWarnings("unchecked")
        Condition<T> cond = new Condition<>((T) this);
        cond.expression(expression);
        clauses.add(new AbstractMap.SimpleEntry<>(Condition.Clause.FIRST, cond));
        return cond;
    }

    /**
     * Factory method for creating a new nested group of the same concrete type.
     * Subclasses should override to return the correct type.
     * 
     * @return a new child condition group
     */
    @SuppressWarnings("unchecked")
    protected T newGroup() {
        return (T) new ConditionGroup<>((T) this); // fallback
    }

    /**
     * Opens a nested child group for more complex conditions.
     * 
     * @return the new nested group
     */
    public T open() {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before open().");
        T g = newGroup();
        addGroup(g, Condition.Clause.FIRST_GROUP);
        return g;
    }

    /** Closes this group and returns its parent group. */
    public T close() { return this.parent; }

    
    /** 
     * Represents an OR operator for adding new conditions or groups.
     * Can chain multiple OR conditions.
     */
    public class OR {
        private final T group;
        public OR(T group) { this.group = group; }

        public Condition<T> column(String columnName) {
            Condition<T> cond = new Condition<>(group);
            group.addCondition(cond, Condition.Clause.OR);
            return cond.column(columnName);
        }
        public Condition<T> expression(String expression) {
            Condition<T> cond = new Condition<>(group);
            group.addCondition(cond, Condition.Clause.OR);
            return cond.expression(expression);
        }
        public T open() {
            T child = group.newGroup();
            group.addGroup(child, Condition.Clause.OR_GROUP);
            return child;
        }
    }
    /** 
     * Represents an AND operator for adding new conditions or groups.
     * Can chain multiple AND conditions.
     */
    public class AND {
        private final T group;
        public AND(T group) { this.group = group; }

        public Condition<T> column(String columnName) {
            Condition<T> cond = new Condition<>(group);
            group.addCondition(cond, Condition.Clause.AND);
            return cond.column(columnName);
        }
        public Condition<T> expression(String expression) {
            Condition<T> cond = new Condition<>(group);
            group.addCondition(cond, Condition.Clause.AND);
            return cond.expression(expression);
        }
        public T open() {
            T child = group.newGroup();
            group.addGroup(child, Condition.Clause.AND_GROUP);
            return group.open();
        }
    }

    /** Returns an OR operator for this group. */
    @SuppressWarnings("unchecked")
    public OR OR() { return new OR((T) this); }
    /** Returns an AND operator for this group. */
    @SuppressWarnings("unchecked")
    public AND AND() { return new AND((T) this); }

    /**
     * Evaluates this condition group for a row of data against the schema.
     * Combines child conditions and nested groups according to AND/OR logic.
     * 
     * @param entryValues the row data
     * @param schema the schema for column indices
     * @return true if all conditions are satisfied
     */
    public boolean isTrue(Object[] entryValues, SchemaInner schema) {
        boolean result = false;
        int groupIndex = 0;
        for (Map.Entry<Clause, Condition<T>> entry : clauses) {
            boolean condResult = entry.getKey().isGroup() 
                ? groups.get(groupIndex++).isTrue(entryValues, schema)
                : entry.getValue().isTrue(entryValues, schema);
            switch (entry.getKey()) {
                case FIRST -> result = condResult;
                case AND -> result = result && condResult;
                case OR -> result = result || condResult;
                case FIRST_GROUP -> result = condResult;
                case OR_GROUP -> result = result || condResult;
                case AND_GROUP -> result = result && condResult;
            }
        }
        return result;
    }
    /**
     * Validates all conditions and nested groups against a schema.
     * 
     * @param schema the schema to validate against
     * @return true if all conditions and groups are valid
     */
    public boolean isValid(Schema schema) {
        // Check all direct conditions
        for (Map.Entry<Clause, Condition<T>> entry : this.clauses) {
            Condition<T> cond = entry.getValue();
            if (cond != null && !cond.isValid(schema)) return false; // invalid condition
        }
        // Check all nested groups
        for (T group : this.groups) {
            if (!group.isValid(schema)) return false; // invalid nested group
        }
        return true; // everything valid
    }
    /** Returns all conditions and their clauses. */
    public List<Map.Entry<Condition.Clause, Condition<T>>> getConditions() { return this.clauses; }
}