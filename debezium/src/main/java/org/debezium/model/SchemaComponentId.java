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
public interface SchemaComponentId extends Identifier {
    
    public enum ComponentType { ENTITY_TYPE }
    
    ComponentType type();
    
}
