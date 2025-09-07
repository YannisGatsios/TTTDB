package com.database.db.api;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.database.db.api.Condition.Clause;
import com.database.db.table.SchemaInner;

public class ConditionGroup<T extends ConditionGroup<T>> {
    private final List<Map.Entry<Condition.Clause, Condition<T>>> clauses = new ArrayList<>();
    private final List<T> groups = new ArrayList<>();
    protected T parent;

    public ConditionGroup() {}
    public ConditionGroup(T parent) { this.parent = parent; }

    protected void addGroup(T group, Condition.Clause clause) {
        this.clauses.add(new AbstractMap.SimpleEntry<>(clause, null));
        this.groups.add(group);
    }
    protected void addCondition(Condition<T> condition, Condition.Clause clause) {
        this.clauses.add(new AbstractMap.SimpleEntry<>(clause, condition));
    }

    // when creating a Cond<T> we must cast this to T — safe if subclassify correctly
    public Condition<T> column(String columnName) {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before column()");
        @SuppressWarnings("unchecked")
        Condition<T> cond = new Condition<>((T) this);
        cond.column(columnName);
        clauses.add(new AbstractMap.SimpleEntry<>(Condition.Clause.FIRST, cond));
        return cond;
    }

    public Condition<T> expression(String expression) {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before expression()");
        @SuppressWarnings("unchecked")
        Condition<T> cond = new Condition<>((T) this);
        cond.expression(expression);
        clauses.add(new AbstractMap.SimpleEntry<>(Condition.Clause.FIRST, cond));
        return cond;
    }

    // factory for child groups: subclasses should override to produce same concrete type
    @SuppressWarnings("unchecked")
    protected T newGroup() {
        return (T) new ConditionGroup<>((T) this); // fallback
    }

    public T open() {
        if (!groups.isEmpty() || !clauses.isEmpty()) throw new IllegalArgumentException("Must use OR()/AND() before open().");
        T g = newGroup();
        addGroup(g, Condition.Clause.FIRST_GROUP);
        return g;
    }

    public T close() { return this.parent; }

    // OR/AND inner classes accept T
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

    @SuppressWarnings("unchecked")
    public OR OR() { return new OR((T) this); }
    @SuppressWarnings("unchecked")
    public AND AND() { return new AND((T) this); }

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
    public List<Map.Entry<Condition.Clause, Condition<T>>> getConditions() { return this.clauses; }
}