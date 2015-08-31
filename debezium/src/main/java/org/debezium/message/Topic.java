/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import org.debezium.annotation.Immutable;

/**
 * The collection of Debezium message topics.
 * 
 * @author Randall Hauch
 */
@Immutable
public class Topic {

    public static final String SCHEMA_PATCHES = "schema-patches";
    public static final String SCHEMA_UPDATES = "schema-updates";
    public static final String SCHEMA_LEARNING = "schema-learning";
    public static final String ENTITY_BATCHES = "entity-batches";
    public static final String ENTITY_PATCHES = "entity-patches";
    public static final String ENTITY_UPDATES = "entity-updates";
    public static final String ENTITY_TYPE_UPDATES = "entity-type-updates";
    public static final String PARTIAL_RESPONSES = "partial-responses";
    public static final String COMPLETE_RESPONSES = "complete-responses";
    public static final String CONNECTIONS = "connections";
    public static final String ZONE_CHANGES = "zone-changes";
    public static final String CHANGES_BY_DEVICE = "changes-by-device";
    public static final String REQUEST_NOTIFICATIONS = "request-notifications";
    public static final String METRICS = "metrics";

    private Topic() {
    }

}
