/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.debezium.core.util.Iterators;

/**
 * @author Randall Hauch
 *
 */
final class BasicDocument implements Document {

    static final Function<Map.Entry<? extends CharSequence, Value>, Field> CONVERT_ENTRY_TO_FIELD = new Function<Map.Entry<? extends CharSequence, Value>, Field>() {
        @Override
        public Field apply(Entry<? extends CharSequence, Value> entry) {
            return new BasicField(entry.getKey(), entry.getValue());
        }
    };

    private final Map<CharSequence, Value> fields = new LinkedHashMap<>();

    BasicDocument() {
    }

    @Override
    public int size() {
        return fields.size();
    }

    @Override
    public boolean isEmpty() {
        return fields.isEmpty();
    }

    @Override
    public int compareTo(Document that) {
        if (that == null) return 1;
        if (this.size() != that.size()) {
            return this.size() - that.size();
        }
        return fields.entrySet()
                     .stream()
                     .mapToInt(e -> e.getValue().compareTo(that.get(e.getKey())))
                     .filter(i -> i != 0)
                     .findFirst()
                     .orElse(0);
    }

    @Override
    public Iterable<CharSequence> keySet() {
        return fields.keySet();
    }

    @Override
    public Iterator<Field> iterator() {
        return Iterators.around(fields.entrySet(), CONVERT_ENTRY_TO_FIELD);
    }

    @Override
    public void clear() {
        fields.clear();
    }

    @Override
    public boolean has(CharSequence fieldName) {
        return fields.containsKey(fieldName);
    }

    @Override
    public boolean hasAll(Document that) {
        if (that == null) return true;
        if (this.size() < that.size()) {
            // Can't have all of 'that' if 'that' is bigger ...
            return false;
        }
        return fields.entrySet().stream().allMatch(entry -> entry.getValue().equals(that.get(entry.getKey())));
    }

    @Override
    public Value get(CharSequence fieldName, Comparable<?> defaultValue) {
        Value value = fields.get(fieldName);
        return value != null ? value : Value.create(defaultValue);
    }

    @Override
    public Document putAll(Iterable<Field> object) {
        object.forEach(this::setValue);
        return this;
    }

    @Override
    public Document removeAll() {
        fields.clear();
        return this;
    }

    @Override
    public Value remove(CharSequence name) {
        if (!fields.containsKey(name)) return null;
        Comparable<?> removedValue = fields.remove(name);
        return Value.create(removedValue);
    }

    @Override
    public Document setValue(CharSequence name, Value value) {
        this.fields.put(name, value != null ? value.clone() : Value.nullValue());
        return this;
    }

    @Override
    public Document clone() {
        return new BasicDocument().putAll(this);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BasicDocument) {
            BasicDocument that = (BasicDocument) obj;
            return fields.equals(that.fields);
        }
        if (obj instanceof Document) {
            Document that = (Document) obj;
            return this.hasAll(that) && that.hasAll(this);
        }
        return false;
    }

    @Override
    public String toString() {
        try {
            return DocumentWriter.prettyWriter().write(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
