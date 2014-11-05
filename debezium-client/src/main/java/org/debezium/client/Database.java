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

    void readSchema(OutcomeHandler<Schema> handler);

    void readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler);

    default void readEntity(EntityId entityId, OutcomeHandler<Stream<Entity>> handler) {
        readEntities(Collections.singleton(entityId), handler);
    }
    
    void changeEntities( Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId,Entity>>> handler );

    @Override
    void close();

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
