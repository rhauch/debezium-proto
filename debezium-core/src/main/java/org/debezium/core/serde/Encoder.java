/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.serde;

/**
 * A function that encodes an object into a byte array.
 * 
 * @param <T> the type of object to encode
 */
@FunctionalInterface
public interface Encoder<T> {
    /**
     * Encode the object into a byte array.
     * 
     * @param value the value to encode; never null
     * @return the bytes; never null
     */
    byte[] toBytes(T value);
}