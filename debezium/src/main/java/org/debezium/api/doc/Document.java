/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api.doc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.debezium.core.doc.BasicDocument;
import org.debezium.core.doc.BasicField;

/**
 * @author Randall Hauch
 *
 */
public interface Document extends Iterable<Document.Field>, Comparable<Document> {

    static interface Field extends Comparable<Field> {

        /**
         * Get the name of the field
         * 
         * @return the field's name; never null
         */
        CharSequence getName();

        /**
         * Get the value of the field.
         * 
         * @return the field's value; may be null
         */
        Value getValue();

        @Override
        default int compareTo(Field that) {
            if (that == null) return 1;
            int diff = this.getName().toString().compareTo(that.getName().toString());
            if (diff != 0) return diff;
            return this.getValue().compareTo(that.getValue());
        }
    }
    
    static Field field( String name, Value value ) {
        return new BasicField(name,value);
    }

    static Field field( String name, Object value ) {
        return new BasicField(name,Value.create(value));
    }

    static Document create() {
        return new BasicDocument();
    }

    static Document create(CharSequence fieldName, Object value) {
        return new BasicDocument().set(fieldName, value);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3, Object value3) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3);
    }

    static Document create(CharSequence fieldName1, Object value1, CharSequence fieldName2, Object value2, CharSequence fieldName3, Object value3, CharSequence fieldName4, Object value4) {
        return new BasicDocument().set(fieldName1, value1).set(fieldName2, value2).set(fieldName3, value3).set(fieldName4, value4);
    }

    /**
     * Return the number of name-value fields in this object.
     * 
     * @return the number of name-value fields; never negative
     */
    int size();

    /**
     * Return whether this document contains no fields and is therefore empty.
     * 
     * @return true if there are no fields in this document, or false if there is at least one.
     */
    boolean isEmpty();

    /**
     * Determine if this contains a field with the given name.
     * 
     * @param fieldName The name of the field
     * @return true if the field exists, or false otherwise
     */
    boolean has(CharSequence fieldName);

    /**
     * Checks if this object contains all of the fields in the supplied document.
     * 
     * @param document The document with the fields that should be in this document
     * @return true if this document contains all of the fields in the supplied document, or false otherwise
     */
    boolean hasAll(Document document);

    /**
     * Gets the value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The field value, if found, or null otherwise
     */
    default Value get(CharSequence fieldName) {
        return get(fieldName, null);
    }

    /**
     * Gets the value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field
     * @return The field value, if found, or , or <code>defaultValue</code> if there is no such field
     */
    Value get(CharSequence fieldName, Comparable<?> defaultValue);

    /**
     * Get the boolean value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The boolean field value, if found, or null if there is no such field or if the value is not a boolean
     */
    default Boolean getBoolean(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isBoolean() ? value.asBoolean() : null;
    }

    /**
     * Get the boolean value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a boolean
     * @return The boolean field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         boolean
     */
    default boolean getBoolean(CharSequence fieldName,
            boolean defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBoolean() ? value.asBoolean().booleanValue() : defaultValue;
    }

    /**
     * Get the integer value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The integer field value, if found, or null if there is no such field or if the value is not an integer
     */
    default Integer getInteger(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isInteger() ? value.asInteger() : null;
    }

    /**
     * Get the integer value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a integer
     * @return The integer field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         integer
     */
    default int getInteger(CharSequence fieldName,
            int defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isInteger() ? value.asInteger().intValue() : defaultValue;
    }

    /**
     * Get the integer value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The long field value, if found, or null if there is no such field or if the value is not a long value
     */
    default Long getLong(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isLong() ? value.asLong() : null;
    }

    /**
     * Get the long value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a long value
     * @return The long field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a long
     *         value
     */
    default long getLong(CharSequence fieldName,
            long defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isLong() ? value.asLong().longValue() : defaultValue;
    }

    /**
     * Get the double value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The double field value, if found, or null if there is no such field or if the value is not a double
     */
    default Double getDouble(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isDouble() ? value.asDouble() : null;
    }

    /**
     * Get the double value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a double
     * @return The double field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         double
     */
    default double getDouble(CharSequence fieldName,
            double defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isDouble() ? value.asDouble().doubleValue() : defaultValue;
    }

    /**
     * Get the double value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The double field value, if found, or null if there is no such field or if the value is not a double
     */
    default Float getFloat(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isFloat() ? value.asFloat() : null;
    }

    /**
     * Get the float value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a double
     * @return The double field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         double
     */
    default float getFloat(CharSequence fieldName,
            float defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isFloat() ? value.asFloat().floatValue() : defaultValue;
    }

    /**
     * Get the number value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The number field value, if found, or null if there is no such field or if the value is not a number
     */
    default Number getNumber(CharSequence fieldName) {
        return getNumber(fieldName, null);
    }

    /**
     * Get the number value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a number
     * @return The number field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         number
     */
    default Number getNumber(CharSequence fieldName,
            Number defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isNumber() ? value.asNumber() : defaultValue;
    }

    /**
     * Get the big integer value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The big integer field value, if found, or null if there is no such field or if the value is not a big integer
     */
    default BigInteger getBigInteger(CharSequence fieldName) {
        return getBigInteger(fieldName, null);
    }

    /**
     * Get the big integer value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a big integer
     * @return The big integer field value, if found, or null if there is no such field or if the value is not a big integer
     */
    default BigInteger getBigInteger(CharSequence fieldName, BigInteger defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBigInteger() ? value.asBigInteger() : defaultValue;
    }

    /**
     * Get the big decimal value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The big decimal field value, if found, or null if there is no such field or if the value is not a big decimal
     */
    default BigDecimal getBigDecimal(CharSequence fieldName) {
        return getBigDecimal(fieldName, null);
    }

    /**
     * Get the big decimal value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a big decimal
     * @return The big decimal field value, if found, or null if there is no such field or if the value is not a big decimal
     */
    default BigDecimal getBigDecimal(CharSequence fieldName, BigDecimal defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isBigDecimal() ? value.asBigDecimal() : defaultValue;
    }

    /**
     * Get the string value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The string field value, if found, or null if there is no such field or if the value is not a string
     */
    default String getString(CharSequence fieldName) {
        return getString(fieldName, null);
    }

    /**
     * Get the string value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default value to return if there is no such field or if the value is not a string
     * @return The string field value if found, or <code>defaultValue</code> if there is no such field or if the value is not a
     *         string
     */
    default String getString(CharSequence fieldName,
            String defaultValue) {
        Value value = get(fieldName);
        return value != null && value.isString() ? value.asString() : defaultValue;
    }

    /**
     * Get the Base64 encoded binary value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The binary field value, if found, or null if there is no such field or if the value is not a binary value
     */
    default byte[] getBytes(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isBinary() ? value.asBytes() : null;
    }

    /**
     * Get the array value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The array field value (as a list), if found, or null if there is no such field or if the value is not an array
     */
    default Array getArray(CharSequence fieldName) {
        return getArray(fieldName,null);
    }

    /**
     * Get the array value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @param defaultValue the default array that should be returned if there is no such field
     * @return The array field value (as a list), if found, or the default value if there is no such field or if the value is not an array
     */
    default Array getArray(CharSequence fieldName, Array defaultValue ) {
        Value value = get(fieldName);
        return value != null && value.isArray() ? value.asArray() : defaultValue;
    }

    /**
     * Get the existing array value in this document for the given field name, or create a new array if there is no existing array
     * at this field.
     * 
     * @param fieldName The name of the field
     * @return The editable array field value; never null
     */
    default Array getOrCreateArray(CharSequence fieldName) {
        Value value = get(fieldName);
        if (value == null) {
            value = Value.create(Array.create());
            set(fieldName, value);
        }
        return value.isArray() ? value.asArray() : null;
    }

    /**
     * Get the document value in this document for the given field name.
     * 
     * @param fieldName The name of the field
     * @return The document field value, if found, or null if there is no such field or if the value is not a document
     */
    default Document getDocument(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isDocument() ? value.asDocument() : null;
    }

    /**
     * Get the existing document value in this document for the given field name, or create a new document if there is no existing
     * document at this field.
     * 
     * @param fieldName The name of the field
     * @return The editable document field value; never null
     */
    default Document getOrCreateDocument(CharSequence fieldName) {
        Value value = get(fieldName);
        if (value == null) {
            value = Value.create(Document.create());
            set(fieldName, value);
        }
        return value.isDocument() ? value.asDocument() : null;
    }

    /**
     * Determine whether this object has a field with the given the name and the value is null. This is equivalent to calling:
     * 
     * <pre>
     * this.get(name) instanceof Null;
     * </pre>
     * 
     * @param fieldName The name of the field
     * @return <code>true</code> if the field exists but is null, or false otherwise
     * @see #isNullOrMissing(CharSequence)
     */
    default boolean isNull(CharSequence fieldName) {
        Value value = get(fieldName);
        return value != null && value.isNull();
    }

    /**
     * Determine whether this object has a field with the given the name and the value is null, or if this object has no field with
     * the given name. This is equivalent to calling:
     * 
     * <pre>
     * Null.matches(this.get(name));
     * </pre>
     * 
     * @param fieldName The name of the field
     * @return <code>true</code> if the field value for the name is null or if there is no such field.
     * @see #isNull(CharSequence)
     */
    default boolean isNullOrMissing(CharSequence fieldName) {
        Value value = get(fieldName);
        return value == null || value.isNull();
    }

    /**
     * Returns this object's fields' names
     * 
     * @return The names of the fields in this object
     */
    Iterable<CharSequence> keySet();

    /**
     * Obtain a clone of this document.
     * 
     * @return the clone of this document; never null
     */
    Document clone();

    /**
     * Remove the field with the supplied name, and return the value.
     * 
     * @param name The name of the field
     * @return the value that was removed, or null if there was no such value
     */
    Value remove(CharSequence name);

    /**
     * Remove all fields from this document.
     * 
     * @return This document, to allow for chaining methods
     */
    Document removeAll();

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     * 
     * @param fields the name/value pairs to be set on this object
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Iterator<Field> fields) {
        if (fields != null) {
            while ( fields.hasNext() ) {
                Field field = fields.next();
                set(field.getName(), field.getValue());
            }
        }
        return this;
    }

    /**
     * Sets on this object all name/value pairs from the supplied object. If the supplied object is null, this method does
     * nothing.
     * 
     * @param fields the name/value pairs to be set on this object
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Iterable<Field> fields) {
        if (fields != null) {
            for (Field field : fields) {
                set(field.getName(), field.getValue());
            }
        }
        return this;
    }

    /**
     * Sets on this object all key/value pairs from the supplied map. If the supplied map is null, this method does nothing.
     * 
     * @param fields the map containing the name/value pairs to be set on this object
     * @return This document, to allow for chaining methods
     */
    default Document putAll(Map<? extends CharSequence, Object> fields) {
        if (fields != null) {
            for (Map.Entry<? extends CharSequence, Object> entry : fields.entrySet()) {
                set(entry.getKey(), entry.getValue());
            }
        }
        return this;
    }

    /**
     * Returns a sequential {@code Stream} with this array as its source.
     *
     * @return a sequential {@code Stream} over the elements in this collection
     */
    default Stream<Field> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Transform all of the field values using the supplied {@link BiFunction transformer function}.
     * 
     * @param transformer the transformer that should be used to transform each field value; may not be null
     * @return this document with transformed fields, or this document if the transformer changed none of the values
     */
    default Document transform(BiFunction<CharSequence, Value, Value> transformer) {
        for (Field field : this) {
            Value existing = get(field.getName());
            Value updated = transformer.apply(field.getName(), existing);
            if (updated == null) updated = Value.nullValue();
            if (updated != existing) {
                setValue(field.getName(), updated);
            }
        }
        return this;
    }

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     * 
     * @param name The name of the field
     * @param value the new value
     * @return This document, to allow for chaining methods
     */
    default Document set(CharSequence name,
            Object value) {
        if (value instanceof Value) {
            setValue(name, (Value)value);
            return this;
        }
        Value wrapped = Value.create(value);
        set(name, wrapped);
        return this;

    }

    /**
     * Set the value for the field with the given name to be a null value. The {@link #isNull(CharSequence)} methods can be used to
     * determine if a field has been set to null, or {@link #isNullOrMissing(CharSequence)} if the field has not be set or if it has
     * been set to null.
     * 
     * @param name The name of the field
     * @return This document, to allow for chaining methods
     * @see #isNull(CharSequence)
     * @see #isNullOrMissing(CharSequence)
     */
    default Document setNull(CharSequence name) {
        setValue(name, Value.nullValue());
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied boolean value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setBoolean(CharSequence name,
            boolean value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied integer value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            int value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied long value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            long value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied float value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            float value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied double value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            double value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            BigInteger value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied big integer value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setNumber(CharSequence name,
            BigDecimal value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to the supplied string value.
     * 
     * @param name The name of the field
     * @param value the new value for the field
     * @return This document, to allow for chaining methods
     */
    default Document setString(CharSequence name,
            String value) {
        setValue(name, Value.create(value));
        return this;
    }

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     * 
     * @param name The name of the field
     * @param data the bytes for the binary value
     * @return This document, to allow for chaining methods
     */
    default Document setBinary(CharSequence name,
            byte[] data) {
        setValue(name, Value.create(data));
        return this;
    }

    /**
     * Set the value for the field with the given name to be a binary value. The value will be encoded as Base64.
     * 
     * @param name The name of the field
     * @param value the new value
     * @return This document, to allow for chaining methods
     */
    Document setValue(CharSequence name,
            Value value);

    /**
     * Set the value for the field with the given name to be a new, empty Document.
     * 
     * @param name The name of the field
     * @return The editable document that was just created; never null
     */
    default Document setDocument(CharSequence name) {
        return setDocument(name, Document.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied Document.
     * 
     * @param name The name of the field
     * @param document the document; if null, a new document will be created
     * @return The document that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>document</code>.
     */
    default Document setDocument(CharSequence name,
            Document document) {
        if (document == null) document = Document.create();
        setValue(name, Value.create(document));
        return document;
    }

    /**
     * Set the value for the field with the given name to be a new, empty array.
     * 
     * @param name The name of the field
     * @return The array that was just created; never null
     */
    default Array setArray(CharSequence name) {
        return setArray(name,Array.create());
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     * 
     * @param name The name of the field
     * @param array the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(CharSequence name,
            Array array) {
        if (array == null) array = Array.create();
        setValue(name, Value.create(array));
        return array;
    }

    /**
     * Set the value for the field with the given name to be the supplied array.
     * 
     * @param name The name of the field
     * @param values the (valid) values for the array
     * @return The array that was just set as the value for the named field; never null and may or may not be the same
     *         instance as the supplied <code>array</code>.
     */
    default Array setArray(CharSequence name,
            Object... values) {
        return setArray(name,Value.create(Array.create(values)));
    }
}