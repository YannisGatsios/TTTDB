package com.database.tttdb.api;

import com.database.tttdb.api.Condition.WhereClause;
import com.database.tttdb.api.DBMS.DeleteQuery;
import com.database.tttdb.api.DBMS.SelectQuery;
import com.database.tttdb.api.DBMS.UpdateQuery;
/**
 * Entry point for the fluent SQL-like query builder.
 * <p>
 * The {@code Query} interface provides a common contract for
 * query types that support a {@link WhereClause}.  
 * Each nested class represents a specific SQL statement:
 * <ul>
 *     <li>{@link Query.Select} – SELECT … FROM … WHERE …</li>
 *     <li>{@link Query.Insert} – INSERT INTO … VALUES …</li>
 *     <li>{@link Query.Delete} – DELETE FROM … WHERE …</li>
 *     <li>{@link Query.Update} – UPDATE … SET … WHERE …</li>
 * </ul>
 * <p>
 * Typical usage:
 * <pre>{@code
 * // SELECT id,username FROM users WHERE age > 18 LIMIT 10
 * Select q = new Query.Select("id,username")
 *        .from("users")
 *        .where().column("age").greaterThan(18).end()
 *        .limit(10)
 *        .get();
 * db.select(q);
 * }</pre>
 */
public interface Query {
    /**
     * Sets the {@link WhereClause} produced by the fluent
     * {@code where()} builder. Implemented by query types that
     * accept a WHERE condition.
     *
     * @param whereClause a completed where clause, may be {@code null}
     */
    void set(WhereClause whereClause);
    /**
     * Indicates how the result set should be ordered when executing a SELECT query.
     * <ul>
     *   <li>{@link #NORMAL} – No explicit ordering (default).</li>
     *   <li>{@link #ASCENDING} – Sort the results in ascending order based on the
     *       default sort column or the database’s natural ordering.</li>
     *   <li>{@link #DESCENDING} – Sort the results in descending order based on the
     *       default sort column or the database’s natural ordering.</li>
     * </ul>
     */
    public enum SelectionType{
        NORMAL,
        ASCENDING,
        DESCENDING,
    }
    public record  SelectType(SelectionType type, String column) {}
    // ---------------------------------------------------------------------
    // SELECT
    // ---------------------------------------------------------------------

    /**
     * Fluent builder for a SELECT query.
     * <p>
     * Start with the list of columns to retrieve, then
     * chain calls to {@link #from(String)}, {@link #where()},
     * {@link #begin(int)}, {@link #limit(int)}, and optionally
     * {@link #ASC()} or {@link #DEC()} to set a {@link SelectionType}.
     * Call {@link #get()} to produce an immutable {@link SelectQuery}.
     */
    public static class Select implements Query{
        private final String selectColumns;
        private String tableName;
        private WhereClause whereClause;
        private int begin = 0;
        private int limit = -1;
        private SelectType type = new SelectType(SelectionType.NORMAL, null);
        /**
         * @param selectColumns comma-separated list of columns to select
         */
        public Select(String selectColumns){
            this.selectColumns = selectColumns;
        }
        /**
         * Specifies the table to query.
         * @param tableName table name
         * @return this builder
         */
        public Select from(String tableName){
            this.tableName = tableName;
            return this;
        }
        /**
         * Starts a fluent WHERE clause.
         * @return a {@link WhereClause} builder linked to this query
         */
        public WhereClause where(){
            return new WhereClause(this);
        }
        /** {@inheritDoc} */
        @Override
        public void set(WhereClause whereClause){
            this.whereClause = whereClause;
        }
        /**
         * Offset of the first row to return (default 0).
         */
        public Select begin(int begin){
            this.begin = begin;
            return this;
        }
        /**
         * Maximum number of rows to return (-1 means no limit).
         */
        public Select limit(int limit){
            this.limit = limit;
            return this;
        }
        /**
         * Sets the {@link SelectionType} to {@link SelectionType#ASCENDING}.
         * @return this builder
         */
        public Select ASC(String column){
            this.type = new SelectType(SelectionType.ASCENDING,column);
            return this;
        }
        /**
         * Sets the {@link SelectionType} to {@link SelectionType#DESCENDING}.
         * @return this builder
         */
        public Select DEC(String column){
            this.type = new SelectType(SelectionType.DESCENDING, column);
            return this;
        }
        /**
         * Builds the final {@link SelectQuery} to pass to {@code DBMS.select()}.
         * @return immutable {@link SelectQuery} with all configured options,
         *         including {@link SelectionType}
         */
        public SelectQuery get(){
            return new SelectQuery(tableName,selectColumns,whereClause,begin,limit,type);
        }
    }
    // ---------------------------------------------------------------------
    // DELETE
    // ---------------------------------------------------------------------

    /**
     * Fluent builder for a DELETE query.
     * <p>
     * Example:
     * <pre>{@code
     * Delete q = new Query.Delete()
     *        .from("users")
     *        .where().column("username").isEqual("bob").end()
     *        .limit(1)
     *        .get();
     * int removed = db.delete(q);
     * }</pre>
     */
    public static class Delete implements Query{
        private String tableName;
        private WhereClause whereClause;
        private int limit = -1;
        public Delete(){}
        /**
         * Table from which to delete rows.
         */
        public Delete from(String tableName){
            this.tableName = tableName;
            return this;
        }
        /**
         * Begins a WHERE clause.
         */
        public WhereClause where(){
            return new WhereClause(this);
        }
        /** {@inheritDoc} */
        @Override
        public void set(WhereClause whereClause){
            this.whereClause = whereClause;
        }
        /**
         * Optional maximum number of rows to delete (-1 means all).
         */
        public Delete limit(int limit){
            this.limit = limit;
            return this;
        }
        /**
         * Builds the final {@link DeleteQuery}.
         */
        public DeleteQuery get(){
            return new DeleteQuery(tableName,whereClause,limit);
        }
    }
    // ---------------------------------------------------------------------
    // UPDATE
    // ---------------------------------------------------------------------

    /**
     * Fluent builder for an UPDATE query.
     * <p>
     * Example:
     * <pre>{@code
     * Update q = new Query.Update("users")
     *        .where().column("id").isEqual(42).end()
     *        .set().selectColumn("username").upperCase()
     *        .limit(1)
     *        .get();
     * db.update(q);
     * }</pre>
     */
    public static class Update implements Query{
        private String tableName;
        private WhereClause whereClause;
        private UpdateFields updateFields;
        private int limit = -1;
        /**
         * @param tableName table to update
         */
        public Update(String tableName){
            this.tableName = tableName;
        }
        /**
         * Begins a WHERE clause to filter rows.
         */
        public WhereClause where(){
            return new WhereClause(this);
        }
        /**
         * Starts an {@link UpdateFields} builder for column updates.
         */
        public UpdateFields set(){
            return new UpdateFields(this);
        }
        /** {@inheritDoc} */
        @Override
        public void set(WhereClause whereClause){
            this.whereClause = whereClause;
        }
        /**
         * Attaches the prepared {@link UpdateFields} list.
         * Usually called internally by {@link UpdateFields}.
         */
        public void set(UpdateFields updateFields){
            this.updateFields = updateFields;
        }
        /**
         * Optional limit on number of rows updated (-1 = no limit).
         */
        public Update limit(int limit){
            this.limit = limit;
            return this;
        }
        /**
         * Builds the final {@link UpdateQuery}.
         */
        public UpdateQuery get(){
            return new UpdateQuery(tableName,whereClause,limit,updateFields);
        }
    }
}