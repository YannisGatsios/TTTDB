package com.database.db.api;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.database.db.api.Condition.UpdateCondition;
import com.database.db.api.Functions.*;
import com.database.db.api.Query.Update;
/**
 * Represents a set of update operations to be applied to one or more columns in a table.
 * <p>
 * Each method adds a transformation or operation to the update chain, and updates are
 * applied **in the exact order they are added** when executed.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>
 * UpdateFields updates = new UpdateFields("username")
 *     .upperCase()           // convert to uppercase
 *     .trim()                // trim whitespace
 *     .replace("OLD", "NEW") // replace substring
 *     .set("FINAL");         // finally set a new value
 * </pre>
 *
 * <p>This class also supports conditional updates:</p>
 * <pre>
 * updates.condition("age")
 *        .isBigger(18)
 *        .endConditionalUpdate();
 * </pre>
 *
 * <p>Common operations include:</p>
 * <ul>
 *     <li>Setting a specific value: {@link #set(Object)}</li>
 *     <li>Setting the default value: {@link #setDefault()}</li>
 *     <li>Arithmetic or expression-based updates: {@link #operation(String)}</li>
 *     <li>Date/time operations: {@link #currentTimestamp()}, {@link #dateAdd(String, long)}, {@link #dateSubtract(String, long)}</li>
 *     <li>String manipulation: {@link #concat(Object[])}, {@link #upperCase()}, {@link #lowerCase()}, {@link #trim()}, {@link #substring(int, int)}, {@link #left(int)}, {@link #right(int)}, {@link #replace(String, String)}, {@link #regexpReplace(String, String)}, {@link #leftPad(int, String)}, {@link #rightPad(int, String)}, {@link #reverse()}</li>
 *     <li>Conditional updates: {@link #condition(String)}, {@link #endConditionalUpdate()}</li>
 * </ul>
 */
public class UpdateFields {
    private final List<InnerFunctions> functionsList;
    private Update update;
    /**
     * Creates an UpdateFields for a specific column.
     *
     * @param column the column name to update
     */
    public UpdateFields(String column){
        this.functionsList = new ArrayList<>();
        this.functionsList.add(new selectColumn(column));
    }
    /**
     * Creates an empty UpdateFields instance. Columns can be added later using {@link #selectColumn(String)}.
     */
    public UpdateFields(){
        this.functionsList = new ArrayList<>();
    }
    public UpdateFields(Update update){
        this.functionsList = new ArrayList<>();
        this.update = update;
    }
    /**
     * Adds a column to be updated.
     * @param column the column name
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields selectColumn(String column){
        this.functionsList.add(new selectColumn(column));
        return this;
    }
    /**
     * Sets a specific value for the selected column(s).
     * @param value the value to set
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields set(Object value){
        InnerFunctions function = new setData(value);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Sets the column(s) to their default value.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields setDefault(){
        InnerFunctions function = new setDefault();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Applies a computed operation to the column(s).
     * @param expression a string expression for the operation
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields operation( String expression){
        InnerFunctions function = new operationData(expression);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Sets the column(s) to the current timestamp.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields currentTimestamp(){
        InnerFunctions function = new currentTimestamp();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Adds an amount to a date/time column.
     * @param unit the unit of time (e.g., "days", "hours")
     * @param amount the amount to add
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields dateAdd( String unit, long amount){
        InnerFunctions function = new dateAdd(unit, amount);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Subtracts an amount from a date/time column.
     * @param unit the unit of time (e.g., "days", "hours")
     * @param amount the amount to subtract
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields dateSubtract( String unit, long amount){
        InnerFunctions function = new dateSub(unit, amount);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Extracts a specific part of a date/time column (e.g., year, month, day).
     * @param part the part to extract
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields extractPart( String part){
        InnerFunctions function = new extractPart(part);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Formats a date column using a specific pattern.
     * @param pattern the date format pattern
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields formatDate( String pattern){
        InnerFunctions function = new formatDate(pattern);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Calculates the difference between two dates.
     * @param from the start date
     * @param to the end date
     * @param unit the unit of difference (e.g., "days", "hours")
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields dateDifference( LocalDateTime from, LocalDateTime to, String unit){
        InnerFunctions function = new dateDifference(from, to, unit);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Concatenates multiple parts into a string.
     * @param parts the parts to concatenate
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields concat( Object[] parts){
        InnerFunctions function = new concat(parts);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Converts the value of a column to upper case.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields upperCase(){
        InnerFunctions function = new upperCase();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Converts the value of a column to lower case.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields lowerCase(){
        InnerFunctions function = new lowerCase();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Trims leading and trailing whitespace from the value.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields trim(){
        InnerFunctions function =  new trim();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Extracts a substring from a column's value.
     * @param start the starting index
     * @param length the length of the substring
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields substring( int start, int length){
        InnerFunctions function =  new substring(start,length);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Keeps the leftmost {@code length} characters of the column value.
     * If the value is shorter than {@code length}, it is left unchanged.
     *
     * @param length the number of characters to keep from the left
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields left( int length){
        InnerFunctions function = new left(length);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Keeps the rightmost {@code length} characters of the column value.
     * If the value is shorter than {@code length}, it is left unchanged.
     *
     * @param length the number of characters to keep from the right
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields right( int length){
        InnerFunctions function = new right(length);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Replaces all occurrences of {@code target} with {@code replacement} in the column value.
     *
     * @param target the substring to replace
     * @param replacement the replacement string
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields replace( String target, String replacement){
        InnerFunctions function = new replace(target, replacement);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Replaces all substrings matching the regular expression {@code regex} with {@code replacement}.
     *
     * @param regex the regular expression pattern
     * @param replacement the replacement string
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields regexpReplace( String regex, String replacement){
        InnerFunctions function = new regexpReplace(regex, replacement);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Pads the value on the left with the specified string until it reaches {@code totalWidth}.
     *
     * @param totalWidth the desired total length of the resulting string
     * @param padStr the string to pad with
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields leftPad( int totalWidth, String padStr){
        InnerFunctions function = new leftPad(totalWidth, padStr);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Pads the value on the right with the specified string until it reaches {@code totalWidth}.
     *
     * @param totalWidth the desired total length of the resulting string
     * @param padStr the string to pad with
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields rightPad(int totalWidth, String padStr){
        InnerFunctions function = new rightPad(totalWidth, padStr);
        this.functionsList.add(function);
        return this;
    }
    /**
     * Reverses the characters in the column value.
     *
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields reverse(){
        InnerFunctions function = new reverse();
        this.functionsList.add(function);
        return this;
    }
    /**
     * Starts a conditional update for the given column.
     * @param column the column name to apply the condition on
     * @return an {@link UpdateCondition} object to define the conditional logic
     */
    public UpdateFields condition(String column){
        UpdateCondition updateCondition = new UpdateCondition(this);
        this.functionsList.add(updateCondition);
        return this;
    }
    /**
     * Ends a conditional update block.
     * @return this UpdateFields instance for fluent chaining
     */
    public UpdateFields endConditionalUpdate(){
        this.functionsList.add(new endConditionalUpdate());
        return this;
    }
    public Update endUpdate(){
        this.update.set(this);
        return this.update;
    }
    /**
     * Returns the internal list of functions in the order they were added.
     * <p>Used internally to apply updates sequentially.</p>
     * @return the ordered list of functions
     */
    public List<InnerFunctions> getFunctionsList() {
        return functionsList;
    }
}