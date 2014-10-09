/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import org.debezium.api.Database;
import org.debezium.api.Entity;
import org.debezium.api.EntityCollection;
import org.debezium.api.EntityId;
import org.debezium.api.EntityType;
import org.debezium.api.message.Batch;

/**
 * A lightweight connection to a database.
 * @author Randall Hauch
 *
 */
final class DatabaseConnection implements Database {
    
    private final DbzDatabases dbs;
    private final ExecutionContext context;
    private volatile boolean isClosed = false;
    
    DatabaseConnection(DbzDatabases dbs,
                       ExecutionContext context) {
        this.dbs = dbs;
        this.context = context;
        assert context != null;
        assert dbs != null;
    }
    
    ExecutionContext getContext() {
        return context;
    }
    
    @Override
    public void readSchema(OutcomeHandler<ReadCollections> handler) {
        ensureOpen();
        dbs.readSchema(context, handler);
    }
    
    @Override
    public void readSchema(EntityType type, OutcomeHandler<EntityCollection> handler) {
        ensureOpen();
        dbs.readSchema(context, type, handler);
    }
    
    @Override
    public void changeSchema(Batch<EntityType> request, OutcomeHandler<ChangeCollections> handler) {
        ensureOpen();
        dbs.changeSchema(context, request, handler);
    }
    
    @Override
    public void readEntities(Iterable<EntityId> entityIds, OutcomeHandler<ReadEntities> handler) {
        ensureOpen();
        dbs.readEntities(context, entityIds, handler);
    }
    
    @Override
    public void readEntity(EntityId entityId, OutcomeHandler<Entity> handler) {
        ensureOpen();
        dbs.readEntity(context, entityId, handler);
    }
    
    @Override
    public void changeEntities(Batch<EntityId> request, OutcomeHandler<ChangeEntities> handler) {
        ensureOpen();
        dbs.changeEntities(context, request, handler);
    }
    
    @Override
    public void loadData() {
        ensureOpen();
        dbs.loadData(context);
    }
    
    
    @Override
    public synchronized void close() {
        if ( !isClosed && dbs.disconnect(this) ) {
            isClosed = true;
        }
    }
    
    private void ensureOpen() {
        if ( isClosed ) throw new IllegalStateException("This database connection for " + context + " is closed");
    }
}
