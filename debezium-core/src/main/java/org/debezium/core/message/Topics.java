/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

/**
 * @author Randall Hauch
 *
 */
public class Topics {

    public static final String SCHEMA_BATCHES = "schema-batches";
    public static final String SCHEMA_UPDATES = "schema-updates";
    public static final String ENTITY_BATCHES = "entity-batches";
    public static final String ENTITY_PATCHES = "entity-patches";
    public static final String ENTITY_UPDATES = "entity-updates";
    public static final String RESPONSES = "responses";
    
    private Topics() {
    }
    
}
