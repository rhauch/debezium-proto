/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.debezium.message.Array;
import org.debezium.message.Value;
import org.fest.assertions.BooleanAssert;
import org.fest.assertions.DoubleAssert;
import org.fest.assertions.FloatAssert;
import org.fest.assertions.GenericAssert;
import org.fest.assertions.IntAssert;
import org.fest.assertions.LongAssert;
import org.fest.assertions.StringAssert;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Formatting.format;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class ArrayAssert extends GenericAssert<ArrayAssert, Array> {

    /**
     * Creates a new {@link ArrayAssert}.
     * 
     * @param actual the target to verify.
     */
    public ArrayAssert(Array actual) {
        super(ArrayAssert.class, actual);
    }

    public ArrayAssert hasEntry(int index, Object value) {
        isNotNull();
        validateKeyNotNull(value);
        if (actual.size() <= index) {
            throw failure(format("expected array had at least <%s> entries but found <%s>", index, actual.size()));
        }
        Value foundValue = actual.get(index);
        if (foundValue.equals(value)) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry %s was <%s> but found <%s>", index, value, foundValue));
    }

    public ValueAssert hasEntry( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null ) return new ValueAssert(value);
        failIfCustomMessageIsSet();
        throw failure(format("expected array has entry <%s>", index));
    }

    public DocumentAssert hasDocumentAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isDocument() ) return new DocumentAssert(value.asDocument());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public ArrayAssert hasArrayAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isArray() ) return new ArrayAssert(value.asArray());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public IntAssert hasIntegerAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isInteger() ) return assertThat(value.asInteger().intValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public LongAssert hasLongAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isLong() ) return assertThat(value.asLong().longValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public BooleanAssert hasBooleanAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isBoolean() ) return assertThat(value.asBoolean().booleanValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public FloatAssert hasFloatAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isFloat() ) return assertThat(value.asFloat().floatValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public DoubleAssert hasDoubleAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isDouble() ) return assertThat(value.asDouble().doubleValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }

    public StringAssert hasStringAt( int index ) {
        isNotNull();
        Value value = actual.get(index);
        if ( value != null && value.isString() ) return assertThat(value.asString());
        failIfCustomMessageIsSet();
        throw failure(format("expected array entry <%s> is an integer", index));
    }
    
    void validateValueNotNull(Object value) {
        if (value == null)
            throw new NullPointerException(format(rawDescription(), "unexpected null value"));
    }

    void validateKeyNotNull(Object value) {
        if (value == null)
            throw new NullPointerException(format(rawDescription(), "unexpected null key"));
    }
}
