/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.io.Closeable;
import java.util.Collections;
import java.util.stream.Stream;

import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;

/**
 * @author Randall Hauch
 *
 */
public interface Database extends Closeable {

    void readEntities( Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler );
    
    default void readEntity( EntityId entityId, OutcomeHandler<Stream<Entity>> handler ) {
        readEntities(Collections.singleton(entityId),handler);
    }

    @Override
    void close();
    
    
    @FunctionalInterface
    public static interface OutcomeHandler<T> {
        void handle(Outcome<T> outcome);
    }
    
    public static interface Outcome<T> {
        public static enum Status { OK, TIMEOUT, CLIENT_STOPPED }
        
        default boolean failed() { return !succeeded(); }
        default boolean succeeded() { return status() != Status.OK; }
        Status status();
        String failureReason();
        T result();
    }
}
