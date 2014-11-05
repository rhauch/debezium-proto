/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identified;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;

/**
 * @author Randall Hauch
 *
 */
public interface Database extends Closeable {

    Completion readSchema(OutcomeHandler<Schema> handler);

    Completion readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler);

    default Completion readEntity(EntityId entityId, OutcomeHandler<Stream<Entity>> handler) {
        return readEntities(Collections.singleton(entityId), handler);
    }
    
    Completion changeEntities( Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId,Entity>>> handler );

    boolean isConnected();
    
    @Override
    void close();
    
    public static interface Completion {
        /**
         * Waits if necessary for the operation to complete, and then
         * retrieves its result.
         *
         * @throws InterruptedException if the current thread was interrupted
         * while waiting
         */
        void await() throws InterruptedException;

        /**
         * Waits if necessary for at most the given time for the operation
         * to complete, and then retrieves its result, if available.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the timeout argument
         * @throws InterruptedException if the current thread was interrupted
         * while waiting
         * @throws TimeoutException if the wait timed out
         */
        void await(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException;

    }

    @FunctionalInterface
    public static interface OutcomeHandler<T> {
        void handle(Outcome<T> outcome);
    }

    public static interface Outcome<T> {
        public static enum Status {
            OK, TIMEOUT, CLIENT_STOPPED, COMMUNICATION_ERROR
        }

        default boolean failed() {
            return !succeeded();
        }

        default boolean succeeded() {
            return status() != Status.OK;
        }

        Status status();

        String failureReason();

        T result();
    }
    
    public static enum ChangeStatus {
        OK, DOES_NOT_EXIST, PATCH_FAILED;
    }

    public static interface Change<IdType extends Identifier,TargetType extends Identified<IdType>> {

        IdType id();
        default boolean failed() {
            return !succeeded();
        }

        default boolean succeeded() {
            return status() != ChangeStatus.OK;
        }

        ChangeStatus status();
        Stream<String> failureReasons();
        Patch<IdType> patch();
        TargetType target();
    }
}
