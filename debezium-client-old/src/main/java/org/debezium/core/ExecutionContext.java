/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import org.debezium.core.component.DatabaseId;

/**
 * @author Randall Hauch
 *
 */
public final class ExecutionContext {

    private final DatabaseId dbId;
    private final String username;
    
    ExecutionContext( DatabaseId dbId, String username ) {
        this.username = username;
        this.dbId = dbId;
    }
    
    public DatabaseId databaseId() {
        return dbId;
    }
    
    public String username() {
        return username;
    }
    
    @Override
    public String toString() {
        return username();
    }
    
}
