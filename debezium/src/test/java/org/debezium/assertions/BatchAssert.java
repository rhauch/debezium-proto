/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.debezium.message.Batch;
import org.debezium.model.Identifier;
import org.fest.assertions.GenericAssert;
import org.fest.assertions.IntAssert;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class BatchAssert extends GenericAssert<BatchAssert, Batch<? extends Identifier>> {

    /**
     * Creates a new {@link BatchAssert}.
     * 
     * @param actual the target to verify.
     */
    public BatchAssert(Batch<? extends Identifier> actual) {
        super(BatchAssert.class, actual);
    }

    public IntAssert hasSize() {
        isNotNull();
        return assertThat(actual.patchCount());
    }
}
