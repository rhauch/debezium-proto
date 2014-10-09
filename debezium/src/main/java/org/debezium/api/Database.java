/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import java.io.Closeable;

import org.debezium.api.message.Batch;
import org.debezium.api.message.Patch;

/**
 * @author Randall Hauch
 *
 */
public interface Database extends Closeable {
    
    void loadData();

    void changeEntities( Batch<EntityId> request, OutcomeHandler<ChangeEntities> handler );

    void readEntities( Iterable<EntityId> entityIds, OutcomeHandler<ReadEntities> handler);

    void readEntity( EntityId entityId, OutcomeHandler<Entity> handler);

    void readSchema( OutcomeHandler<ReadCollections> handler);

    void readSchema( EntityType type, OutcomeHandler<EntityCollection> handler);

    void changeSchema( Batch<EntityType> request, OutcomeHandler<ChangeCollections> handler );
    
    @Override
    void close();
    
    
    @FunctionalInterface
    public static interface OutcomeHandler<T> {
        void handle(Outcome<T> outcome);
    }
    
    public static interface Outcome<T> {
        default boolean failed() { return !succeeded(); }
        default boolean succeeded() { return cause() == null; }
        ChangeFailureCode failureCode();
        Throwable cause();
        T result();
    }
    
    public static interface ReadMultiple<IdType extends Identifier,TargetType extends Identified<IdType>> extends Iterable<TargetType> {
        int size();
        boolean isEmpty();
        TargetType get( IdType id );
    }
    
    public static interface ReadEntities extends ReadMultiple<EntityId,Entity> {
    }
    
    public static interface ReadCollections extends ReadMultiple<EntityType,EntityCollection> {
    }
    
    public static interface ChangeEntities extends Iterable<ChangeEntity> {
        int size();
        boolean isEmpty();
    }
    
    public static interface ChangeCollections extends Iterable<ChangeCollection> {
        int size();
        boolean isEmpty();
    }
    
    public static interface ChangeIdentified<IdType extends Identifier,TargetType extends Identified<IdType>> {
        IdType id();
        boolean succeeded();
        boolean failed();
        String failureReason();
        ChangeFailureCode failureCode();
        Patch<IdType> failedPatch();
        TargetType unmodifiedTarget();
    }
    
    public static interface ChangeEntity extends ChangeIdentified<EntityId,Entity> {
    }
    
    public static interface ChangeCollection extends ChangeIdentified<EntityType,EntityCollection> {
    }
    
    public enum OutcomeFailureCode {
        TIMEOUT, RECIPIENT_FAILURE;
    }
    
    public enum ChangeFailureCode {
        PREVIOUSLY_DELETED, PRECONDITIONS_NOT_SATISFIED, SCHEMA_NOT_SATISFIED, OPERATION_FAILED;
    }
    
}
