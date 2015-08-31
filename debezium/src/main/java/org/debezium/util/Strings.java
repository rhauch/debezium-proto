/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.debezium.annotation.ThreadSafe;

/**
 * String-related utility methods.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class Strings {

    /**
     * Represents a predicate (boolean-valued function) of one character argument.
     */
    @FunctionalInterface
    public static interface CharacterPredicate {
        /**
         * Evaluates this predicate on the given character argument.
         *
         * @param c the input argument
         * @return {@code true} if the input argument matches the predicate, or {@code false} otherwise
         */
        boolean test(char c);
    }

    /**
     * Compare two {@link CharSequence} instances.
     * 
     * @param str1 the first character sequence; may be null
     * @param str2 the second character sequence; may be null
     * @return a negative integer if the first sequence is less than the second, zero if the sequence are equivalent (including if
     *         both are null), or a positive integer if the first sequence is greater than the second
     */
    public static int compareTo(CharSequence str1, CharSequence str2) {
        if (str1 == str2) return 0;
        if (str1 == null) return -1;
        if (str2 == null) return 1;
        return str1.toString().compareTo(str2.toString());
    }

    /**
     * Trim away any leading or trailing whitespace characters.
     * <p>
     * This is semantically equivalent to {@link String#trim()} but instead uses {@link #trim(String, CharacterPredicate)}.
     * 
     * @param str the string to be trimmed; may not be null
     * @return the trimmed string; never null
     * @see #trim(String,CharacterPredicate)
     */
    public static String trim(String str) {
        return trim(str, c -> c <= ' ');    // same logic as String.trim()
    }

    /**
     * Trim away any leading or trailing characters that satisfy the supplied predicate
     * 
     * @param str the string to be trimmed; may not be null
     * @param predicate the predicate function; may not be null
     * @return the trimmed string; never null
     * @see #trim(String)
     */
    public static String trim(String str, CharacterPredicate predicate) {
        int len = str.length();
        if (len == 0) return str;
        int st = 0;
        while ((st < len) && predicate.test(str.charAt(st))) {
            st++;
        }
        while ((st < len) && predicate.test(str.charAt(len - 1))) {
            len--;
        }
        return ((st > 0) || (len < str.length())) ? str.substring(st, len) : str;
    }

    /**
     * Get the stack trace of the supplied exception.
     * 
     * @param throwable the exception for which the stack trace is to be returned
     * @return the stack trace, or null if the supplied exception is null
     */
    public static String getStackTrace(Throwable throwable) {
        if (throwable == null) return null;
        final ByteArrayOutputStream bas = new ByteArrayOutputStream();
        final PrintWriter pw = new PrintWriter(bas);
        throwable.printStackTrace(pw);
        pw.close();
        return bas.toString();
    }

    /**
     * Parse the supplied string as a integer value.
     * 
     * @param value the string representation of a integer value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as an int
     * @return the int value
     */
    public static int asInt(String value, int defaultValue) {
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Parse the supplied string as a long value.
     * 
     * @param value the string representation of a long value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a long
     * @return the long value
     */
    public static long asLong(String value, long defaultValue) {
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Parse the supplied string as a double value.
     * 
     * @param value the string representation of a double value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a double
     * @return the double value
     */
    public static double asDouble(String value, double defaultValue) {
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    /**
     * Parse the supplied string as a boolean value.
     * 
     * @param value the string representation of a boolean value
     * @param defaultValue the value to return if the string value is null or cannot be parsed as a boolean
     * @return the boolean value
     */
    public static boolean asBoolean(String value, boolean defaultValue) {
        if (value != null) {
            try {
                return Boolean.parseBoolean(value);
            } catch (NumberFormatException e) {
            }
        }
        return defaultValue;
    }

    private Strings() {
    }
}
