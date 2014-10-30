/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

/**
 * @author Randall Hauch
 *
 */
public final class Strings {
    
    @FunctionalInterface
    public static interface CharacterPredicate {
        boolean test(char c);
    }
    
    public static final CharacterPredicate WHITESPACE = new CharacterPredicate() {
        @Override
        public boolean test(char c) {
            return c <= ' ';
        }
    };
    
    /**
     * Trim away any leading or trailing whitespace characters.
     * <p>
     * This is semantically equivalent to {@link String#trim()} but instead uses {@link #trim(String, CharacterPredicate)}.
     * @param str the string to be trimmed; may not be null
     * @return the trimmed string; never null
     * @see #trim(String,CharacterPredicate)
     */
    public static String trim(String str) {
        return trim(str,WHITESPACE);
    }

    /**
     * Trim away any leading or trailing characters that satisfy the supplied predicate
     * @param str the string to be trimmed; may not be null
     * @param predicate the predicate function; may not be null
     * @return the trimmed string; never null
     * @see #trim(String)
     */
    public static String trim(String str, CharacterPredicate predicate) {
        int len = str.length();
        if ( len == 0 ) return str;
        int st = 0;
        while ((st < len) && predicate.test(str.charAt(st))) {
            st++;
        }
        while ((st < len) && predicate.test(str.charAt(len - 1))) {
            len--;
        }
        return ((st > 0) || (len < str.length())) ? str.substring(st, len) : str;
    }
    
    private Strings() {
    }
}
