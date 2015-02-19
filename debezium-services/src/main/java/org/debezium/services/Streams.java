/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.system.SystemStream;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.ZoneId;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message.Field;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Topic;

/**
 * @author Randall Hauch
 *
 */
public class Streams {

    public static final String SYSTEM_NAME = "debezium";

    private static final SystemStream SCHEMA_PATCHES = new SystemStream(SYSTEM_NAME, Topic.SCHEMA_PATCHES);
    private static final SystemStream SCHEMA_UPDATES = new SystemStream(SYSTEM_NAME, Topic.SCHEMA_UPDATES);
    private static final SystemStream ENTITY_BATCHES = new SystemStream(SYSTEM_NAME, Topic.ENTITY_BATCHES);
    private static final SystemStream ENTITY_PATCHES = new SystemStream(SYSTEM_NAME, Topic.ENTITY_PATCHES);
    private static final SystemStream ENTITY_UPDATES = new SystemStream(SYSTEM_NAME, Topic.ENTITY_UPDATES);
    private static final SystemStream PARTIAL_RESPONSES = new SystemStream(SYSTEM_NAME, Topic.PARTIAL_RESPONSES);
    private static final SystemStream COMPLETE_RESPONSES = new SystemStream(SYSTEM_NAME, Topic.COMPLETE_RESPONSES);
    private static final SystemStream SCHEMA_LEARNING = new SystemStream(SYSTEM_NAME, Topic.SCHEMA_LEARNING);
    private static final SystemStream CONNECTIONS = new SystemStream(SYSTEM_NAME, Topic.CONNECTIONS);
    private static final SystemStream ZONE_CHANGES = new SystemStream(SYSTEM_NAME, Topic.ZONE_CHANGES);
    private static final SystemStream CHANGES_BY_DEVICE = new SystemStream(SYSTEM_NAME, Topic.CHANGES_BY_DEVICE);
    private static final SystemStream REQUEST_NOTIFICATIONS = new SystemStream(SYSTEM_NAME, Topic.REQUEST_NOTIFICATIONS);

    // At this time, none of the stream names are a function of database ID. However, we may want to do this so that individual
    // database info is stored in Kafka within files with database-specific names, making it easier to completely remove all
    // data for the database by removing all of the files associated with that database.

    /**
     * Get the stream for the given database ID that is partitioned by {@link DatabaseId} and used to submit {@link Patch patches}
     * to the database's schema.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream schemaPatches(DatabaseId id) {
        return SCHEMA_PATCHES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link DatabaseId} and used to record successfully-applied
     * {@link Patch patches} to the database's schema.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream schemaUpdates(DatabaseId id) {
        return SCHEMA_UPDATES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link EntityType} and used to submit two kinds of
     * messages:
     * <ol>
     * <li>successfully applied patches (each with the updated entity representation); and</li>
     * <li>read-requests for entity type representations</li>
     * </ol>
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream schemaLearning(DatabaseId id) {
        return SCHEMA_LEARNING;
    }

    /**
     * Get the stream for the given database ID that is partitioned randomly and used to record {@link Batch batches} to entities
     * within a single database.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream entityBatches(DatabaseId id) {
        return ENTITY_BATCHES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link EntityId} and used to record {@link Patch patches}
     * to entities.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream entityPatches(DatabaseId id) {
        return ENTITY_PATCHES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link EntityId} and used to record successfully-applied
     * {@link Patch patches} to entities.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream entityUpdates(DatabaseId id) {
        return ENTITY_UPDATES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link ZoneId} and used to record what's changed within
     * each zone.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream zoneChanges(DatabaseId id) {
        return ZONE_CHANGES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by device and used to record for each device the changes
     * that are to be delivered to it. Note that the changes are simply summaries of whether entities were created, updated,
     * or removed.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream changesByDevice(DatabaseId id) {
        return CHANGES_BY_DEVICE;
    }

    /**
     * Get the stream for the given database ID that is partitioned by device and used to record requests to obtain the which
     * entities were created, updated, and deleted since the last request.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream requestNotifications(DatabaseId id) {
        return REQUEST_NOTIFICATIONS;
    }

    /**
     * Get the stream for the given database ID that is partitioned by username and used to record connections of users with
     * particular devices.
     * 
     * @param id the database ID; may not be null
     * @return the stream; never null
     */
    public static SystemStream connections(DatabaseId id) {
        return CONNECTIONS;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link Field#CLIENT_ID client ID} and used to record
     * individual read requests and successfully-applied {@link Patch patches}. Each message might be one of several parts
     * within a client's {@link Batch batch} request.
     * 
     * @return the stream; never null
     * @see #completeResponses()
     */
    public static SystemStream partialResponses() {
        return PARTIAL_RESPONSES;
    }

    /**
     * Get the stream for the given database ID that is partitioned by {@link Field#CLIENT_ID client ID} and used to output
     * complete batch requests of reads and successfully-applied {@link Patch patches}.
     * 
     * @return the stream; never null
     * @see #partialResponses()
     */
    public static SystemStream completeResponses() {
        return COMPLETE_RESPONSES;
    }

    public static boolean isEntityUpdates(SystemStream stream) {
        return stream.getStream().equals(Topic.ENTITY_UPDATES);
    }

    public static boolean isSchemaUpdates(SystemStream stream) {
        return stream.getStream().equals(Topic.SCHEMA_UPDATES);
    }

    public static boolean isChangesByDevice(SystemStream stream) {
        return stream.getStream().equals(Topic.CHANGES_BY_DEVICE);
    }

    public static boolean isRequestNotifications(SystemStream stream) {
        return stream.getStream().equals(Topic.REQUEST_NOTIFICATIONS);
    }

    private Streams() {
    }

}
