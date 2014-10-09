/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.debezium.api.doc.Array;
import org.debezium.api.doc.Document;
import org.debezium.api.doc.Value;

/**
 * @author Randall Hauch
 *
 */
public final class ComparableValue implements Value {
    
    private static final Map<Class<?>,Type> TYPES_BY_CLASS;
    
    static {
        Map<Class<?>,Type> types = new HashMap<>();
        types.put(String.class, Type.STRING);
        types.put(Boolean.class, Type.BOOLEAN);
        types.put(byte[].class, Type.BINARY);
        types.put(Integer.class, Type.INTEGER);
        types.put(Long.class, Type.LONG);
        types.put(Float.class, Type.FLOAT);
        types.put(Double.class, Type.DOUBLE);
        types.put(BigInteger.class, Type.BIG_INTEGER);
        types.put(BigDecimal.class, Type.DECIMAL);
        types.put(BasicDocument.class, Type.DOCUMENT);
        types.put(BasicArray.class, Type.ARRAY);
        TYPES_BY_CLASS = types;
    }

    private final Comparable<?> value;

    public ComparableValue(Comparable<?> value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof Value) {
            Value that = (Value) obj;
            return this.value.equals(that.asObject());
        }
        // Compare the value straight away ...
        return this.value.equals(obj);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Value that) {
        if (that.isNull()) return 1;
        return ((Comparable<Object>) this.value).compareTo(that.asObject());
    }

    @Override
    public Type getType() {
        Type type = TYPES_BY_CLASS.get(value.getClass());
        if ( type == null ) {
            // Didn't match by exact class, so then figure out the extensible types by instanceof ...
            if ( isDocument() ) return Type.DOCUMENT;
            if ( isArray() ) return Type.ARRAY;
            if ( isNull() ) return Type.NULL;
        }
        assert type != null;
        return type;
    }

    @Override
    public Comparable<?> asObject() {
        return value;
    }

    @Override
    public String asString() {
        return value instanceof String ? (String) value : null;
    }

    @Override
    public Integer asInteger() {
        return value instanceof Integer ? (Integer) value : null;
    }

    @Override
    public Long asLong() {
        return value instanceof Long ? (Long) value : null;
    }

    @Override
    public Boolean asBoolean() {
        return value instanceof Boolean ? (Boolean) value : null;
    }

    @Override
    public Number asNumber() {
        return value instanceof Number ? (Number) value : null;
    }

    @Override
    public BigInteger asBigInteger() {
        return value instanceof BigInteger ? (BigInteger) value : null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return value instanceof BigDecimal ? (BigDecimal) value : null;
    }

    @Override
    public Float asFloat() {
        return value instanceof Float ? (Float) value : null;
    }

    @Override
    public Double asDouble() {
        return value instanceof Double ? (Double) value : null;
    }

    @Override
    public Document asDocument() {
        return value instanceof Document ? (Document) value : null;
    }

    @Override
    public Array asArray() {
        return value instanceof Array ? (Array) value : null;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isString() {
        return value instanceof String;
    }

    @Override
    public boolean isBoolean() {
        return value instanceof Boolean;
    }

    @Override
    public boolean isInteger() {
        return value instanceof Integer;
    }

    @Override
    public boolean isLong() {
        return value instanceof Long;
    }

    @Override
    public boolean isFloat() {
        return value instanceof Float;
    }

    @Override
    public boolean isDouble() {
        return value instanceof Double;
    }

    @Override
    public boolean isNumber() {
        return value instanceof Number;
    }

    @Override
    public boolean isBigInteger() {
        return value instanceof BigInteger;
    }

    @Override
    public boolean isBigDecimal() {
        return value instanceof BigDecimal;
    }

    @Override
    public boolean isDocument() {
        return value instanceof Document;
    }

    @Override
    public boolean isArray() {
        return value instanceof Array;
    }
    
    @Override
    public boolean isBinary() {
        return false;
    }
    
    @Override
    public byte[] asBytes() {
        return null;
    }

    @Override
    public Value convert() {
        return new ConvertingValue(this);
    }

    @Override
    public Value clone() {
        if (isArray()) {
            return new ComparableValue(asArray().clone());
        }
        if (isDocument()) { // or array
            return new ComparableValue(asDocument().clone());
        }
        // All other values are immutable ...
        return this;
    }

}
