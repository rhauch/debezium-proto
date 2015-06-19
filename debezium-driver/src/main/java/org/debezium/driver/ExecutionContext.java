/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.core.component.DatabaseId;

/**
 * @author Randall Hauch
 *
 */
final class ExecutionContext {

    private final DatabaseId dbId;
    private final String username;
    private final String device;
    private final String version;
    private final long defaultTimeout;
    private final TimeUnit timeoutUnit;
    
    ExecutionContext( DatabaseId dbId, String username, String device, String version, long defaultTimeout, TimeUnit unit ) {
        this.username = username;
        this.dbId = dbId;
        this.device =device;
        this.version = version;
        this.defaultTimeout = defaultTimeout;
        this.timeoutUnit=unit;
    }
    
    public DatabaseId databaseId() {
        return dbId;
    }
    
    public String username() {
        return username;
    }
    
    public String device() {
        return device;
    }
    
    public String version() {
        return version;
    }
    
    public long defaultTimeout() {
        return defaultTimeout;
    }
    
    public TimeUnit timeoutUnit() {
        return timeoutUnit;
    }
    
    @Override
    public String toString() {
        return username();
    }
    
}
