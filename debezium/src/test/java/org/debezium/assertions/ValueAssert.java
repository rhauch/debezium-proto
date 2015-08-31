/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.debezium.message.Value;
import org.fest.assertions.GenericAssert;

import static org.fest.assertions.Formatting.format;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class ValueAssert extends GenericAssert<ValueAssert, Value> {

    /**
     * Creates a new {@link ValueAssert}.
     * 
     * @param actual the target to verify.
     */
    public ValueAssert(Value actual) {
        super(ValueAssert.class, Value.create(actual));
    }
    
    public ValueAssert isType( Value.Type type ) {
        isNotNull();
        validateTypeNotNull(type);
        if ( actual.getType() == type ) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected value %s to be of type <%s> but found <%s>", actual, type, actual.getType()));
    }
    
    public ValueAssert isNullValue() {
        isNotNull();
        if ( actual.isNull() ) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected value %s to be null but found <%s>", actual, actual.getType()));
    }
    
    @Override
    public ValueAssert isEqualTo(Value expected) {
        if ( actual.isNull() && Value.isNull(expected) ) return this;
        return super.isEqualTo(Value.create(expected));
    }
    
    void validateTypeNotNull(Object value) {
        if (value == null)
          throw new NullPointerException(format(rawDescription(),"unexpected null type"));
      }

    void validateKeyNotNull(Object value) {
        if (value == null)
          throw new NullPointerException(format(rawDescription(),"unexpected null key"));
      }
}
