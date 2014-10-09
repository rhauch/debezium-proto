/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import org.debezium.api.doc.Array.Entry;
import org.debezium.api.doc.Value;

/**
 * @author Randall Hauch
 *
 */
final class BasicEntry implements Entry {

    private final int index;
    private final Value value;

    BasicEntry(int index, Value value) {
        this.index = index;
        this.value = value;
    }
    
    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "@" + index + "=" + value;
    }

}
