/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;


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
    public void readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler) {
        ensureOpen();
        dbs.readEntities(context,entityIds,handle(response->{
            EntityId id = Message.getEntityId(response);
            Document representation = Message.getAfter(response);
            return Entity.with(id,representation);
        }));
    }
    
    private <R> ResponseHandlers.Handlers handle( Function<Document,R> processResponse ) {
        OutcomeBuilder<R> builder = new OutcomeBuilder<R>(processResponse);
        return ResponseHandlers.with(builder::accumulate, builder::success, builder::failed);
    }
    
    private static final class OutcomeBuilder<T> {
        private List<T> results = new ArrayList<>();
        private Function<Document,T> responseAdapter;
        protected OutcomeBuilder( Function<Document,T> adapter ) {
            this.responseAdapter = adapter;
        }
        public void accumulate( Document response ) {
            T result = responseAdapter.apply(response);
            if ( result != null ) results.add(result);
        }
        public Outcome<Stream<T>> success() {
            return new Outcome<Stream<T>>() {
                @Override
                public Stream<T> result() {
                    return results.stream();
                }
                @Override
                public Outcome.Status status() {
                    return Outcome.Status.OK;
                }
                @Override
                public String failureReason() {
                    return null;
                }
            };
        }
        public Outcome<Stream<T>> failed( Outcome.Status status, String failureReason ) {
            return new Outcome<Stream<T>>() {
                @Override
                public Stream<T> result() {
                    return null;
                }
                @Override
                public Outcome.Status status() {
                    return status;
                }
                @Override
                public String failureReason() {
                    return failureReason;
                }
            };
        }
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
