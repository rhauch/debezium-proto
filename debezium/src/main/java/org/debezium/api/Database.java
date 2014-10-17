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
    
    void changeEntities( Batch<EntityId> request, OutcomeHandler<ChangeEntities> handler );

    void readEntities( Iterable<EntityId> entityIds, OutcomeHandler<ReadEntities> handler);

    void readEntity( EntityId entityId, OutcomeHandler<Entity> handler);

    void readSchema( OutcomeHandler<ChangeSchemaComponents> handler );

    void changeSchema( Batch<? extends SchemaComponentId> request, OutcomeHandler<ChangeSchemaComponents> handler );

    @Override
    void close();
    
    
    @FunctionalInterface
    public static interface OutcomeHandler<T> {
        void handle(Outcome<T> outcome);
    }
    
    public static interface Outcome<T> {
        public static enum Status { OK, TIMEOUT, RECIPIENT_FAILURE, CLIENT_STOPPED }
        
        default boolean failed() { return !succeeded(); }
        default boolean succeeded() { return status() != Status.OK; }
        Status status();
        String failureReason();
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
    
    public static interface ChangeSchemaComponents extends Iterable<ChangeCollection> {
        int size();
        boolean isEmpty();
    }
    
    public static interface ChangeIdentified<IdType extends Identifier,TargetType extends Identified<IdType>> {
        public static enum Status {
            OK, PREVIOUSLY_DELETED, PRECONDITIONS_NOT_SATISFIED, SCHEMA_NOT_SATISFIED, OPERATION_FAILED, CLIENT_STOPPED;
        }

        IdType id();
        boolean succeeded();
        boolean failed();
        String failureReason();
        Status status();
        Patch<IdType> failedPatch();
        TargetType unmodifiedTarget();
    }
    
    public static interface ChangeSchemaComponent extends ChangeIdentified<SchemaComponentId,SchemaComponent<SchemaComponentId>> {
    }
    
    public static interface ChangeEntity extends ChangeIdentified<EntityId,Entity> {
    }
    
    public static interface ChangeCollection extends ChangeIdentified<EntityType,EntityCollection> {
    }
    
    
}
