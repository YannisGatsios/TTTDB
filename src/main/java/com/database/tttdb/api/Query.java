package com.database.tttdb.api;

import java.util.List;

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
    enum SelectionType{
        NORMAL,
        ASCENDING,
        DESCENDING,
    }
    record  SelectType(SelectionType type, String column) {}
    // ---------------------------------------------------------------------
    // SELECT
    // ---------------------------------------------------------------------

    /**
     * Fluent builder for a SELECT query.
     * <p>
     * Start with the list of columns to retrieve, then
     * chain calls to {@link #from(String)}, {@link #where()},
     * {@link #begin(int)}, {@link #limit(int)}, and optionally
     * {@link #ASC(String columnName)} or {@link #DEC(String columnName)} to set a {@link SelectionType}.
     * Call {@link #fetch()} to produce an immutable {@link SelectQuery}.
     */
    class Select implements Query{
        private final DBMS dbms;
        private final String selectColumns;
        private String tableName;
        private WhereClause whereClause;
        private int begin = 0;
        private int limit = -1;
        private SelectType type = new SelectType(SelectionType.NORMAL, null);
        /**
         * @param selectColumns comma-separated list of columns to select
         */
        public Select(DBMS dbms, String selectColumns){
            this.dbms = dbms;
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
         * Executes the configured SELECT query and returns the resulting rows.
         * <p>
         * Builds a final immutable {@link SelectQuery} and passes it to
         * {@link DBMS#select(SelectQuery)} for execution.
         * </p>
         *
         * @return a list of {@link Row} objects matching the query
         * @throws IllegalArgumentException if no database is currently selected
         */
        public List<Row> fetch(){
            return dbms.select(new SelectQuery(tableName,selectColumns,whereClause,begin,limit,type));
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
    class Delete implements Query{
        private final DBMS dbms;
        private String tableName;
        private WhereClause whereClause;
        private int limit = -1;
        public Delete(DBMS dbms){
            this.dbms = dbms;
        }
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
         * Executes the configured DELETE query and returns the number of deleted rows.
         * <p>
         * Builds an immutable {@link DeleteQuery} and passes it to
         * {@link DBMS#delete(DeleteQuery)} for execution.
         * </p>
         *
         * @return the number of deleted entries affected by the query
         * @throws IllegalArgumentException if no database is currently selected
         */
        public int execute(){
            return dbms.delete(new DeleteQuery(tableName,whereClause,limit));
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
    class Update implements Query{
        private final DBMS dbms;
        private final String tableName;
        private WhereClause whereClause;
        private UpdateFields updateFields;
        private int limit = -1;
        /**
         * @param tableName table to update
         */
        public Update(DBMS dbms, String tableName){
            this.dbms = dbms;
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
         * Executes the configured UPDATE query and returns the number of affected rows.
         * <p>
         * Builds an immutable {@link UpdateQuery} and passes it to
         * {@link DBMS#update(UpdateQuery)} for execution.
         * </p>
         *
         * @return the number of updated entries
         * @throws IllegalArgumentException if no database is currently selected
         */
        public int execute(){
            return dbms.update(new UpdateQuery(tableName,whereClause,limit,updateFields));
        }
    }
}