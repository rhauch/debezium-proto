/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import org.debezium.core.doc.Document;

/**
 * @author Randall Hauch
 *
 */
public class Schema {
    
    public static Document getOrCreateComponent(SchemaComponentId componentId, Document schema) {
        Document component = null;
        switch (componentId.type()) {
            case ENTITY_TYPE:
                EntityType type = (EntityType) componentId;
                Document collections = schema.getOrCreateDocument("collections");
                component = collections.getOrCreateDocument(type.entityTypeName());
                break;
        }
        return component;
    }
    
    public static Document getComponent(SchemaComponentId componentId, Document schema) {
        Document component = null;
        switch (componentId.type()) {
            case ENTITY_TYPE:
                EntityType type = (EntityType) componentId;
                Document collections = schema.getDocument("collections");
                if (collections != null) {
                    component = collections.getDocument(type.entityTypeName());
                }
                break;
        }
        return component;
    }
    
    private Schema() {
    }
    
}
