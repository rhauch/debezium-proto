/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import org.debezium.api.doc.Document.Field;
import org.debezium.api.doc.Value;

/**
 * @author Randall Hauch
 *
 */
public final class BasicField implements Field {

    private final CharSequence name;
    private final Value value;

    public BasicField(CharSequence name, Value value) {
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
