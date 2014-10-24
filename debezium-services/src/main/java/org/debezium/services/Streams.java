/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services;

import org.apache.samza.system.SystemStream;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.message.Topics;

/**
 * @author Randall Hauch
 *
 */
public class Streams {
    
    public static final String SYSTEM_NAME = "debezium-kafka";
    
    private static final SystemStream SCHEMA_BATCHES = new SystemStream(SYSTEM_NAME, Topics.SCHEMA_BATCHES);
    private static final SystemStream SCHEMA_UPDATES = new SystemStream(SYSTEM_NAME, Topics.SCHEMA_UPDATES);
    private static final SystemStream ENTITY_BATCHES = new SystemStream(SYSTEM_NAME, Topics.ENTITY_BATCHES);
    private static final SystemStream ENTITY_PATCHES = new SystemStream(SYSTEM_NAME, Topics.ENTITY_PATCHES);
    private static final SystemStream ENTITY_UPDATES = new SystemStream(SYSTEM_NAME, Topics.ENTITY_UPDATES);
    private static final SystemStream RESPONSES = new SystemStream(SYSTEM_NAME, Topics.RESPONSES);
    private static final SystemStream SCHEMA_LEARNING = new SystemStream(SYSTEM_NAME, "schema-learning");
    
    // At this time, none of the stream names are a function of database ID. However, we may want to do this so that individual
    // database info is stored in Kafka within files with database-specific names, making it easier to completely remove all
    // data for the database by removing all of the files associated with that database.
    
    public static SystemStream schemaBatches(DatabaseId id) {
        return SCHEMA_BATCHES;
    }
    
    public static SystemStream schemaUpdates(DatabaseId id) {
        return SCHEMA_UPDATES;
    }
    
    public static SystemStream schemaLearning(DatabaseId id) {
        return SCHEMA_LEARNING;
    }
    
    public static SystemStream entityBatches(DatabaseId id) {
        return ENTITY_BATCHES;
    }
    
    public static SystemStream entityPatches(DatabaseId id) {
        return ENTITY_PATCHES;
    }
    
    public static SystemStream entityUpdates(DatabaseId id) {
        return ENTITY_UPDATES;
    }
    
    public static SystemStream responses(DatabaseId id) {
        return RESPONSES;
    }
    
    public static boolean isEntityUpdates(SystemStream stream) {
        return stream.getStream().equals(Topics.ENTITY_UPDATES);
    }
    
    public static boolean isSchemaUpdates(SystemStream stream) {
        return stream.getStream().equals(Topics.SCHEMA_UPDATES);
    }
    
    private Streams() {
    }
    
}
