/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

/**
 * @author Randall Hauch
 *
 */
public class ConnectionFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ConnectionFailedException(DatabaseId dbId) {
        this(dbId,null);
    }
    
    public ConnectionFailedException(DatabaseId dbId, Throwable cause) {
        super("Unable to connect to '" + dbId.asString() + "'",cause);
    }
}
