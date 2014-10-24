/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;


/**
 * @author Randall Hauch
 *
 */
final class BasicField implements Document.Field {

    private final CharSequence name;
    private final Value value;

    BasicField(CharSequence name, Value value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return name + "=" + value;
    }

}
