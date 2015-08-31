/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

/**
 * @author Randall Hauch
 *
 */
public final class System {
    
    public static final String INTERNAL_PREFIX = "$";
    
    public static final class EntityTypes {
        public static final String ZONE_SUBSCRIPTION = INTERNAL_PREFIX + "zoneSubscriptions";
    }
    
    /**
     * 
     */
    private System() {
    }

}
