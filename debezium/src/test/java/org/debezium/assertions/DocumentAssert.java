/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import java.util.HashSet;
import java.util.Set;

import org.debezium.message.Array;
import org.debezium.message.Document;
import org.debezium.message.Value;
import org.fest.assertions.Assertions;
import org.fest.assertions.BooleanAssert;
import org.fest.assertions.DoubleAssert;
import org.fest.assertions.FloatAssert;
import org.fest.assertions.GenericAssert;
import org.fest.assertions.IntAssert;
import org.fest.assertions.LongAssert;
import org.fest.assertions.StringAssert;

import static org.debezium.assertions.DebeziumAssertions.assertThat;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Formatting.format;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class DocumentAssert extends GenericAssert<DocumentAssert, Document> {

    /**
     * Creates a new {@link DocumentAssert}.
     * 
     * @param actual the target to verify.
     */
    public DocumentAssert(Document actual) {
        super(DocumentAssert.class, actual);
    }
    
    public BooleanAssert isEmpty() {
        isNotNull();
        return assertThat(actual.isEmpty());
    }
    
    public IntAssert size() {
        isNotNull();
        return assertThat(actual.size());
    }

    public DocumentAssert hasAll(Document doc) {
        isNotNull();
        validateValueNotNull(doc);
        if ( actual.hasAll(doc) ) return this;
        failIfCustomMessageIsSet();
        Set<Document.Field> missing = new HashSet<>();
        doc.forEach(field->{
            Value value = actual.get(field.getName());
            if ( Value.isNull(value) ) missing.add(field);
        });
        if ( missing.isEmpty() ) return this;
        throw failure(format("message is missing fields: %s", missing));
    }

    public DocumentAssert hasNoFieldAt(CharSequence fieldName) {
        isNotNull();
        validateKeyNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (Value.isNull(value)) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have no field <%s> but found <%s>", fieldName, value));
    }

    public DocumentAssert hasField(CharSequence fieldName) {
        isNotNull();
        validateKeyNotNull(fieldName);
        if (actual.has(fieldName)) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have field <%s>", fieldName));
    }

    public DocumentAssert hasField(CharSequence fieldName, Object expected) {
        isNotNull();
        validateKeyNotNull(fieldName);
        validateValueNotNull(expected);
        Value value = actual.get(fieldName);
        if (value != null && value.equals(expected)) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have field <%s> with value <%s> but was <%s>", fieldName, expected, value));
    }

    public ArrayAssert hasArrayAt(CharSequence fieldName) {
        isNotNull();
        validateKeyNotNull(fieldName);
        Array array = actual.getArray(fieldName);
        if (array != null) return new ArrayAssert(array);
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have array <%s>", fieldName));
    }

    public DocumentAssert hasDocumentAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Document doc = actual.getDocument(fieldName);
        if (doc != null) return new DocumentAssert(doc);
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have array <%s>", fieldName));
    }

    public ValueAssert hasValueAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (Value.notNull(value)) return new ValueAssert(value);
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have field <%s>", fieldName));
    }

    public StringAssert hasStringAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isString()) return Assertions.assertThat(value.asString());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have string field <%s> but was <%s>", fieldName, value));
    }

    public IntAssert hasIntegerAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isInteger()) return Assertions.assertThat(value.asInteger().intValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have integer field <%s> but was <%s>", fieldName, value));
    }

    public LongAssert hasLongAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isLong()) return Assertions.assertThat(value.asLong().longValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have integer field <%s> but was <%s>", fieldName, value));
    }

    public BooleanAssert hasBooleanAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isInteger()) return Assertions.assertThat(value.asBoolean().booleanValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have integer field <%s> but was <%s>", fieldName, value));
    }

    public FloatAssert hasFloatAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isInteger()) return Assertions.assertThat(value.asFloat().floatValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have integer field <%s> but was <%s>", fieldName, value));
    }

    public DoubleAssert hasDoubleAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        Value value = actual.get(fieldName);
        if (value != null && value.isInteger()) return Assertions.assertThat(value.asDouble().doubleValue());
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have integer field <%s> but was <%s>", fieldName, value));
    }

    public ArrayAssert arrayAt(CharSequence fieldName) {
        isNotNull();
        validateKeyNotNull(fieldName);
        return assertThat(actual.getArray(fieldName));
    }

    public DocumentAssert documentAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getDocument(fieldName));
    }

    public ValueAssert valueAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.get(fieldName));
    }

    public StringAssert stringAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getString(fieldName));
    }

    public IntAssert integerAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getInteger(fieldName));
    }

    public LongAssert longAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getLong(fieldName));
    }

    public BooleanAssert booleanAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getBoolean(fieldName));
    }

    public FloatAssert floatAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getFloat(fieldName));
    }

    public DoubleAssert doubleAt(CharSequence fieldName) {
        isNotNull();
        validateValueNotNull(fieldName);
        return assertThat(actual.getDouble(fieldName));
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
