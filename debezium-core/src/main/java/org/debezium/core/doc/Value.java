/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author Randall Hauch
 *
 */
public interface Value extends Comparable<Value> {
    
    static enum Type {
        NULL, STRING, BOOLEAN, BINARY, INTEGER, LONG, FLOAT, DOUBLE, BIG_INTEGER, DECIMAL, DOCUMENT, ARRAY;
    }
    
    static boolean isNull( Value value ) {
        return value == null || value.isNull();
    }

    static boolean isValid(Object value) {
        return value == null || value instanceof Value ||
                value instanceof String || value instanceof Boolean ||
                value instanceof Integer || value instanceof Long ||
                value instanceof Float || value instanceof Double ||
                value instanceof Document || value instanceof Array ||
                value instanceof BigInteger || value instanceof BigDecimal;
    }

    static Value create(Object value) {
        if ( !isValid(value) ) {
            assert value != null;
            throw new IllegalArgumentException("Unexpected value " + value + "' of type " + value.getClass());
        }
        return value == null ? NullValue.INSTANCE : new ComparableValue((Comparable<?>)value);
    }

    static Value create(boolean value) {
        return new ComparableValue(Boolean.valueOf(value));
    }

    static Value create(int value) {
        return new ComparableValue(Integer.valueOf(value));
    }

    static Value create(long value) {
        return new ComparableValue(Long.valueOf(value));
    }

    static Value create(float value) {
        return new ComparableValue(Float.valueOf(value));
    }

    static Value create(double value) {
        return new ComparableValue(Double.valueOf(value));
    }

    static Value create(BigInteger value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(BigDecimal value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Integer value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Long value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Float value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Double value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(String value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(byte[] value) {
        return value == null ? NullValue.INSTANCE : new BinaryValue(value);
    }

    static Value create(Document value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value create(Array value) {
        return value == null ? NullValue.INSTANCE : new ComparableValue(value);
    }

    static Value nullValue() {
        return NullValue.INSTANCE;
    }
    
    default Type getType() {
        if ( isString() ) return Type.STRING;
        if ( isBoolean() ) return Type.BOOLEAN;
        if ( isBinary() ) return Type.BINARY;
        if ( isInteger() ) return Type.INTEGER;
        if ( isLong() ) return Type.LONG;
        if ( isFloat() ) return Type.FLOAT;
        if ( isDouble() ) return Type.DOUBLE;
        if ( isBigInteger() ) return Type.BIG_INTEGER;
        if ( isBigDecimal() ) return Type.DECIMAL;
        if ( isDocument() ) return Type.DOCUMENT;
        if ( isArray() ) return Type.ARRAY;
        if ( isNull() ) return Type.NULL;
        assert false;
        throw new IllegalStateException();
    }

    /**
     * Get the raw value.
     * 
     * @return the raw value; may be null
     */
    Object asObject();

    String asString();

    Integer asInteger();

    Long asLong();

    Boolean asBoolean();

    Number asNumber();

    BigInteger asBigInteger();

    BigDecimal asBigDecimal();

    Float asFloat();

    Double asDouble();

    Document asDocument();

    Array asArray();
    
    byte[] asBytes();

    boolean isNull();

    boolean isString();

    boolean isInteger();

    boolean isLong();

    boolean isBoolean();

    boolean isNumber();

    boolean isBigInteger();

    boolean isBigDecimal();

    boolean isFloat();

    boolean isDouble();

    boolean isDocument();

    boolean isArray();
    
    boolean isBinary();

    /**
     * Get a Value representation that will convert attempt to convert values.
     * 
     * @return a value that can convert actual values to the requested format
     */
    Value convert();

    /**
     * Obtain a clone of this value.
     * 
     * @return the clone of this value; never null, but possibly the same instance if the underlying value is immutable
     *         and not a document or array
     */
    Value clone();
}