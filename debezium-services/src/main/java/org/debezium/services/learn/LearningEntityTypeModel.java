/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services.learn;

import java.util.function.Consumer;

import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Patch;

/**
 * A learning model of an entity type, based upon individual changes to entities.
 * @author Randall Hauch
 */
@NotThreadSafe
public class LearningEntityTypeModel {
    
    /**
     * @param model the starting representation of the entity type
     */
    public LearningEntityTypeModel( Document model ) {
    }

    /**
     * Update this model with the given entity patch and representation.
     * @param patch the patch that was successfully applied to the entity; never null
     * @param entityRepresentation the updated representation of the entity; possibly null if it is not known
     * @param updatedEntityType the function that should be called if these updates alter the entity type model; may be null
     */
    public void update( Patch<EntityId> patch, Document entityRepresentation, Consumer<Patch<EntityType>> updatedEntityType ) {
        
    }
    
}
