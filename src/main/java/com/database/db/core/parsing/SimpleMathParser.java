package com.database.db.core.parsing;

public class SimpleMathParser {
    
    private String input;
    private int pos;

    public double parseDouble(String s) {
        input = s;
        pos = 0;
        double result = parseExpressionDouble();
        if (pos < input.length()) {
            throw new RuntimeException("Unexpected: " + input.charAt(pos));
        }
        return result;
    }

    // handles + and -
    private double parseExpressionDouble() {
        double x = parseTermDouble();
        while (true) {
            if (eatDouble('+')) x += parseTermDouble();
            else if (eatDouble('-')) x -= parseTermDouble();
            else return x;
        }
    }

    // handles * / %
    private double parseTermDouble() {
        double x = parseFactorDouble();
        while (true) {
            if (eatDouble('*')) x *= parseFactorDouble();
            else if (eatDouble('/')) x /= parseFactorDouble();
            else if (eatDouble('%')) x %= parseFactorDouble();
            else return x;
        }
    }

    // handles ^ (right-associative), and parentheses
    private double parseFactorDouble() {
        if (eatDouble('+')) return parseFactorDouble(); // unary +
        if (eatDouble('-')) return -parseFactorDouble(); // unary -

        double x;
        int startPos = this.pos;

        if (eatDouble('(')) {
            x = parseExpressionDouble();
            if (!eatDouble(')')) {
                throw new RuntimeException("Expected closing parenthesis at pos " + pos);
            }
        } else if ((input.charAt(pos) >= '0' && input.charAt(pos) <= '9') || input.charAt(pos) == '.') {
            while (pos < input.length() && (
                (input.charAt(pos) >= '0' && input.charAt(pos) <= '9') ||
                input.charAt(pos) == '.'
            )) pos++;
            x = Double.parseDouble(input.substring(startPos, pos));
        } else {
            throw new RuntimeException("Unexpected: " + input.charAt(pos));
        }

        // Handle exponentiation after parsing the base
        if (eatDouble('^')) {
            x = Math.pow(x, parseFactorDouble());
        }

        return x;
    }

    private boolean eatDouble(char charToEat) {
        while (pos < input.length() && input.charAt(pos) == ' ') pos++;
        if (pos < input.length() && input.charAt(pos) == charToEat) {
            pos++;
            return true;
        }
        return false;
    }

        public long parseLong(String s) {
        input = s;
        pos = 0;
        long result = parseExpressionLong();
        if (pos < input.length()) {
            throw new RuntimeException("Unexpected: " + input.charAt(pos));
        }
        return result;
    }

    // handles + and -
    private long parseExpressionLong() {
        long x = parseTermLong();
        while (true) {
            if (eatLong('+')) x += parseTermLong();
            else if (eatLong('-')) x -= parseTermLong();
            else return x;
        }
    }

    // handles * / %
    private long parseTermLong() {
        long x = parseFactorLong();
        while (true) {
            if (eatLong('*')) x *= parseFactorLong();
            else if (eatLong('/')) x /= parseFactorLong();
            else if (eatLong('%')) x %= parseFactorLong();
            else return x;
        }
    }

    // handles ^ (right-associative), and parentheses
    private long parseFactorLong() {
        if (eatLong('+')) return parseFactorLong(); // unary +
        if (eatLong('-')) return -parseFactorLong(); // unary -

        long x;
        int startPos = this.pos;

        if (eatLong('(')) {
            x = parseExpressionLong();
            if (!eatLong(')')) {
                throw new RuntimeException("Expected closing parenthesis at pos " + pos);
            }
        } else if (isDigit(input.charAt(pos))) {
            while (pos < input.length() && isDigit(input.charAt(pos))) pos++;
            String token = input.substring(startPos, pos);
            x = Long.parseLong(token);
        } else {
            throw new RuntimeException("Unexpected: " + input.charAt(pos));
        }

        // Handle exponentiation after parsing the base
        if (eatLong('^')) {
            long exponent = parseFactorLong();
            x = pow(x, exponent);
        }

        return x;
    }

    private long pow(long base, long exp) {
        long result = 1;
        while (exp > 0) {
            if ((exp & 1) == 1) result *= base;
            base *= base;
            exp >>= 1;
        }
        return result;
    }

    private boolean eatLong(char charToEat) {
        while (pos < input.length() && input.charAt(pos) == ' ') pos++;
        if (pos < input.length() && input.charAt(pos) == charToEat) {
            pos++;
            return true;
        }
        return false;
    }

    private boolean isDigit(char c) {
        return (c >= '0' && c <= '9');
    }
}
