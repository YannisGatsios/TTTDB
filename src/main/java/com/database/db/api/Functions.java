package com.database.db.api;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import com.database.db.parsing.SimpleMathParser;
import com.database.db.table.DataType;
import com.database.db.table.Schema;

public class Functions {

    public interface InnerFunctions {
        /**
         * @param schema      describes your table (names, types)
         * @param currentRow  the array of current column‑values
         * @param columnIndex which column you’re updating (for type‑checking)
         * @return the new value
         */
        public Object apply(Schema schema, Object[] data,int columnIndex);
    }

    public record setData(Object value) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data,int columnIndex){
            return Functions.set(schema , value, columnIndex);
        }
    }

    // Number based functions

    public record operationData(String expression) implements InnerFunctions {
        public Object apply(Schema schema, Object[] data,int columnIndex){
            return Functions.getOperationResult(schema, data, expression, columnIndex);
        }
    }

        // Date based functions

    public record currentTimestamp() implements InnerFunctions {
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            return Functions.getCurrentTimestamp();
        }
    }

    public record  dateAdd(String unit, long amount) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.dateAdd(value, unit, amount);
        }
    }

    public record dateSub(String unit, long amount) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.dateSub(value, unit, amount);
        }
    }

    public record extractPart(String part) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.extractPart(value, part);
        }
    }

    public record formatDate(String pattern) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            LocalDateTime value = (LocalDateTime)data[columnIndex];
            return Functions.formatDate(value, pattern);
        }
    }

    public record dateDifference(LocalDateTime from, LocalDateTime to, String unit) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            return Functions.dateDifference(from, to, unit);
        }
    }

    // String based functions

    public record concat(Object[] parts) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            return Functions.concat(parts);
        }
    }

    public record upperCase() implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use upper case to null value.");
            String value = (String)data[columnIndex];
            return value.toUpperCase();
        }
    }

    public record lowerCase() implements InnerFunctions {
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use lower case to null value.");
            String value = (String)data[columnIndex];
            return value.toLowerCase();
        }
    }

    public record trim() implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use trim to null value.");
            String value = (String)data[columnIndex];
            return value.trim();
        }
    }

    public record substring(int start, int length) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use substring to null value.");
            String value = (String)data[columnIndex];
            return Functions.substring(value, start, length);
        }
    }

    public record left(int length) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use left to null value.");
            String value = (String)data[columnIndex];
            return Functions.left(value, length);
        }
    }

    public record right(int length) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use right to null value.");
            String value = (String)data[columnIndex];
            return Functions.right(value, length);
        }
    }

    public record replace(String target, String replacement) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use replace to null value.");
            String value = (String)data[columnIndex];
            return value.replace(target, replacement);
        }
    }

    public record regexpReplace(String regex, String replacement) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use regex to null value.");
            String value = (String)data[columnIndex];
            return Functions.regexpReplace(value, regex, replacement);
        }
    }

    public record leftPad(int totalWidth, String padStr) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use left pad to null value.");
            String value = (String)data[columnIndex];
            return Functions.pad(value, totalWidth, padStr, true);
        }
    }

    public record rightPad(int totalWidth, String padStr) implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not use right pad to null value.");
            String value = (String)data[columnIndex];
            return Functions.pad(value, totalWidth, padStr, false);
        }
    }

    public record reverse() implements InnerFunctions{
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            if (data[columnIndex] == null) 
                throw new NullPointerException("Can not reverse null String.");
            String value = (String)data[columnIndex];
            return new StringBuilder(value).reverse().toString();
        }
    }

    // Conditional
    public record ifNull(Object fallback) implements InnerFunctions {
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            boolean isNull = data[columnIndex] == null;
            if(fallback instanceof InnerFunctions && isNull)
                return ((InnerFunctions)fallback).apply(schema,data,columnIndex);
            return isNull ? fallback : data[columnIndex];
        }
    }

    public record ifNotNull(Object fallback) implements InnerFunctions {
        public Object apply(Schema schema, Object[] data, int columnIndex) {
            boolean isNull = data[columnIndex] == null;
            if(fallback instanceof InnerFunctions && !isNull)
                return ((InnerFunctions)fallback).apply(schema,data,columnIndex);
            return !isNull ? fallback : data[columnIndex];
        }
    }

    // Actual Functions code

    private static Object set(Schema schema, Object value, int columnIndex){
        DataType type = schema.getTypes()[columnIndex];
        if(!type.getJavaClass().isInstance(value))
            throw new IllegalArgumentException("Type mismatch to set expected: "+type.name());
        return value;
    }

    private static Number getOperationResult(Schema schema, Object[] data, String expression, int columnIndex){
        if (expression == null || data == null) return null;
        String[] colNames = schema.getNames();
        // Replace variables in the expression with their values
        for (int i = 0; i < colNames.length; i++) {
            String var = colNames[i];
            Object val = data[i];
            expression = expression.replaceAll("\\b" + var + "\\b", val.toString());
        }
        try {
            SimpleMathParser eval = new SimpleMathParser();
            DataType[] types = schema.getTypes();
            if (types[columnIndex] == DataType.FLOAT || types[columnIndex] == DataType.DOUBLE) { 
                double result = eval.parseDouble(expression);
                return result;
            } else {
                long result = eval.parseLong(expression);
                return result;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
            if (o != null) sb.append(o.toString());
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
