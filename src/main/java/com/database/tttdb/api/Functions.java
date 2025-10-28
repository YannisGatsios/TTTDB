package com.database.tttdb.api;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.database.tttdb.core.parsing.SimpleMathParser;
import com.database.tttdb.core.table.DataType;
import com.database.tttdb.core.table.TableSchema;

public class Functions {

    public interface InnerFunctions {
        /**
         * @param schema       tables schema
         * @param rowData     current row values
         * @param columnIndex index of the column being updated
         * @return the new value
         */
        Object apply(TableSchema schema, Object[] rowData, int columnIndex);
    }

    public record setData(Object value) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data,int columnIndex){
            return Functions.set(schema, value, columnIndex);
        }
    }

    public record setDefault() implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data,int columnIndex){
            return Functions.setDefault(schema, columnIndex);
        }
    }

    // Number based functions

    public record operationData(String expression) implements InnerFunctions {
        public Object apply(TableSchema schema, Object[] data,int columnIndex){
            return Functions.getOperationResult(schema, data, expression, columnIndex);
        }
    }

    // Date based functions

    public record currentTimestamp() implements InnerFunctions {
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            return Functions.getCurrentTimestamp();
        }
    }

    public record  dateAdd(String unit, long amount) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.dateAdd(value, unit, amount);
        }
    }

    public record dateSub(String unit, long amount) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.dateSub(value, unit, amount);
        }
    }

    public record extractPart(String part) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.extractPart(value, part);
        }
    }

    public record formatDate(String pattern) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.formatDate(value, pattern);
        }
    }

    public record dateDifference(LocalDateTime from, LocalDateTime to, String unit) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            return Functions.dateDifference(from, to, unit);
        }
    }

    // String based functions

    public record concat(Object[] parts) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            return Functions.concat(parts);
        }
    }

    public record upperCase() implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use upper case to null value.");
            String value = (String)data[columnIndex];
            return value.toUpperCase();
        }
    }

    public record lowerCase() implements InnerFunctions {
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use lower case to null value.");
            String value = (String)data[columnIndex];
            return value.toLowerCase();
        }
    }

    public record trim() implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use trim to null value.");
            String value = (String)data[columnIndex];
            return value.trim();
        }
    }

    public record substring(int start, int length) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use substring to null value.");
            String value = (String)data[columnIndex];
            return Functions.substring(value, start, length);
        }
    }

    public record left(int length) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use left to null value.");
            String value = (String)data[columnIndex];
            return Functions.left(value, length);
        }
    }

    public record right(int length) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use right to null value.");
            String value = (String)data[columnIndex];
            return Functions.right(value, length);
        }
    }

    public record replace(String target, String replacement) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use replace to null value.");
            String value = (String)data[columnIndex];
            return value.replace(target, replacement);
        }
    }

    public record regexpReplace(String regex, String replacement) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use regex to null value.");
            String value = (String)data[columnIndex];
            return Functions.regexpReplace(value, regex, replacement);
        }
    }

    public record leftPad(int totalWidth, String padStr) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use left pad to null value.");
            String value = (String)data[columnIndex];
            return Functions.pad(value, totalWidth, padStr, true);
        }
    }

    public record rightPad(int totalWidth, String padStr) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use right pad to null value.");
            String value = (String)data[columnIndex];
            return Functions.pad(value, totalWidth, padStr, false);
        }
    }

    public record reverse() implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not reverse null String.");
            String value = (String)data[columnIndex];
            return new StringBuilder(value).reverse().toString();
        }
    }

    // Special

    public record endConditionalUpdate() implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] rowData, int columnIndex) {
            throw new UnsupportedOperationException("Unimplemented method 'apply' for endConditionalUpdate()");
        }
    }
    public record selectColumn(String column) implements InnerFunctions{
        public Object apply(TableSchema schema, Object[] rowData, int columnIndex) {
            throw new UnsupportedOperationException("Unimplemented method 'apply' for selectColumn(String column)");
        }
    }

    // Actual Functions code

    private static Object set(TableSchema schema, Object value, int columnIndex){
        DataType type = schema.getTypes()[columnIndex];
        if(value != null && !type.getJavaClass().isInstance(value))
            throw new IllegalArgumentException("Type mismatch to set expected: " + type.name());
        return value;
    }

    private static Object setDefault(TableSchema schema, int columnIndex){
        return schema.getDefaults()[columnIndex];
    }

    private static Number getOperationResult(TableSchema schema, Object[] data, String expression, int columnIndex){
        if (expression == null || schema == null || data == null) return null;
        // Replace variables with column values (tokenized, safe)
        Pattern tokenPattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
        Matcher matcher = tokenPattern.matcher(expression);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String var = matcher.group();
            int idx = schema.getColumnIndex(var);
            if (idx >= 0) {
                Object val = data[idx];
                matcher.appendReplacement(sb, val.toString());
            }
        }
        matcher.appendTail(sb);

        String exprEval = sb.toString();
        SimpleMathParser eval = new SimpleMathParser();

        try {
            if (columnIndex >= 0) {
                // Normal case: we know the type of the "target column"
                DataType[] types = schema.getTypes();
                if (types[columnIndex] == DataType.FLOAT || types[columnIndex] == DataType.DOUBLE) {
                    return eval.parseDouble(exprEval);
                } else {
                    return eval.parseLong(exprEval);
                }
            } else {
                // Pure expression: default to double
                return eval.parseDouble(exprEval);
            }
        } catch (Exception e) {
            throw new RuntimeException("Invalid expression: " + expression, e);
        }
    }

    private static LocalDateTime getCurrentTimestamp() {
        return LocalDateTime.now();
    }

    private static LocalDateTime dateAdd(LocalDateTime base, String unit, long amount) {
        return switch (unit.toLowerCase()) {
            case "days" -> base.plusDays(amount);
            case "months" -> base.plusMonths(amount);
            case "years" -> base.plusYears(amount);
            case "hours" -> base.plusHours(amount);
            case "minutes" -> base.plusMinutes(amount);
            case "seconds" -> base.plusSeconds(amount);
            default -> base;
        };
    }

    private static LocalDateTime dateSub(LocalDateTime base, String unit, long amount) {
        return dateAdd(base, unit, -amount);
    }

    private static int extractPart(LocalDateTime dateTime, String part) {
        return switch (part.toLowerCase()) {
            case "year" -> dateTime.getYear();
            case "month" -> dateTime.getMonthValue();
            case "day" -> dateTime.getDayOfMonth();
            case "hour" -> dateTime.getHour();
            case "minute" -> dateTime.getMinute();
            case "second" -> dateTime.getSecond();
            default -> throw new IllegalArgumentException("Unknown part: " + part);
        };
    }

    private static String formatDate(LocalDateTime dateTime, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return dateTime.format(formatter);
    }

    private static long dateDifference(LocalDateTime from, LocalDateTime to, String unit) {
        return switch (unit.toLowerCase()) {
            case "days" -> ChronoUnit.DAYS.between(from, to);
            case "months" -> ChronoUnit.MONTHS.between(from, to);
            case "years" -> ChronoUnit.YEARS.between(from, to);
            case "hours" -> ChronoUnit.HOURS.between(from, to);
            case "minutes" -> ChronoUnit.MINUTES.between(from, to);
            case "seconds" -> ChronoUnit.SECONDS.between(from, to);
            default -> throw new IllegalArgumentException("Unknown unit: " + unit);
        };
    }

    // --- implementations of string helpers ---

    private static String concat(Object[] parts) {
        StringBuilder sb = new StringBuilder();
        for (Object o : parts) {
            if (o != null) sb.append(o);
        }
        return sb.toString();
    }

    private static String substring(String s, int start, int length) {
        if (s == null) return null;
        int end = Math.min(s.length(), start + length);
        return s.substring(Math.max(0, start), end);
    }

    private static String left(String s, int length) {
        if (s == null) return null;
        return s.substring(0, Math.min(s.length(), length));
    }

    private static String right(String s, int length) {
        if (s == null) return null;
        int start = Math.max(0, s.length() - length);
        return s.substring(start);
    }

    private static String regexpReplace(String s, String regex, String replacement) {
        if (s == null) return null;
        return s.replaceAll(regex, replacement);
    }

    private static String pad(String s, int totalWidth, String padStr, boolean leftPad) {
        if (s == null) s = "";
        if (padStr == null || padStr.isEmpty()) padStr = " ";
        int padLen = totalWidth - s.length();
        if (padLen <= 0) return s;

        StringBuilder padding = new StringBuilder();
        while (padding.length() < padLen) {
            padding.append(padStr);
        }
        // truncate extra pad
        String pad = padding.substring(0, padLen);

        return leftPad ? pad + s : s + pad;
    }
}