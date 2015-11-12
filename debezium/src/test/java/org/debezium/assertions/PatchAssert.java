/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.debezium.message.Patch;
import org.debezium.model.Identifier;
import org.fest.assertions.GenericAssert;
import org.fest.assertions.IntAssert;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class PatchAssert extends GenericAssert<PatchAssert, Patch<? extends Identifier>> {

    /**
     * Creates a new {@link PatchAssert}.
     * 
     * @param actual the target to verify.
     */
    public PatchAssert(Patch<? extends Identifier> actual) {
        super(PatchAssert.class, actual);
    }

    public IntAssert hasSize() {
        isNotNull();
        return assertThat(actual.operationCount());
    }
}
