/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.function.BiConsumer;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Message.Field;


/**
 * @author Randall Hauch
 *
 */
public class Schema {

    public static void onEachEntityType( Document schema, DatabaseId dbId, BiConsumer<EntityType,Document> consumer ) {
        Document collections = schema.getDocument("collections");
        if ( collections != null ) {
            collections.forEach((field)->{
                Value value = field.getValue();
                if ( value != null && value.isDocument()) {
                    consumer.accept(Identifier.of(dbId,field.getName()), value.asDocument());
                }
            });
        }
    }
    
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
    
    public static void setLearning( Document schema, boolean enabled ) {
        if (enabled) {
            schema.setBoolean(Field.LEARNING, true);
        } else {
            schema.remove(Field.LEARNING);
        }
    }
    
    public static boolean isLearningEnabled( Document schema ) {
        return schema.getBoolean(Field.LEARNING, false);
    }
    
    private Schema() {
    }
    
}
