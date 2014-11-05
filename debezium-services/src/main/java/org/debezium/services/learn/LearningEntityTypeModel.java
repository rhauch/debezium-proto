/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.services.learn;

import java.util.Optional;
import java.util.function.Consumer;

import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.component.EntityCollection;
import org.debezium.core.component.EntityCollection.FieldDefinition;
import org.debezium.core.component.EntityCollection.FieldType;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.SchemaEditor;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Add;
import org.debezium.core.message.Patch.Copy;
import org.debezium.core.message.Patch.Increment;
import org.debezium.core.message.Patch.Move;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Patch.Remove;
import org.debezium.core.message.Patch.Replace;
import org.debezium.core.message.Patch.Require;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A learning model of an entity type, based upon individual changes to entities.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class LearningEntityTypeModel {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LearningEntityTypeModel.class);
    
    private final EntityType type;
    private EntityCollection model;
    private Patch.Editor<Patch<EntityType>> typeEditor;
    private Strategy strategy;
    
    /**
     * Create a new model for the given entity type and starting representation.
     * 
     * @param type the entity type; never null
     * @param model the starting representation of the entity type; never null
     */
    public LearningEntityTypeModel(EntityType type, Document model) {
        this.type = type;
        this.model = EntityCollection.with(type,model);
        this.typeEditor = Patch.edit(type);
        this.strategy = new SimpleFieldStrategy();
    }
    
    /**
     * Update this model based upon the given entity patch and complete representation.
     * 
     * @param patch the patch that was successfully applied to the entity; never null
     * @param entityRepresentation the updated representation of the entity <em>after</em> the patch was applied; possibly null
     *            if it is not known
     * @param updatedEntityType the function that should be called if these updates alter the entity type model; may be null
     */
    public void adapt(Patch<EntityId> patch, Document entityRepresentation, Consumer<Patch<EntityType>> updatedEntityType) {
        // Adapt to the supplied entity patch ...
        patch.forEach((op) -> strategy.handle(op, entityRepresentation, typeEditor, model));
        // Now figure out if anything changed ...
        typeEditor.endIfChanged().ifPresent((entityTypePatch) -> {
            // Update the model ...
            entityTypePatch.apply(model.document(), (failedOp) -> LOGGER.error("Unable to apply {} to model for {}: {}", failedOp, type, model));
            // And call the supplied function ...
            updatedEntityType.accept(entityTypePatch);
        });
    }
    
    protected static interface Strategy {
        
        void handle(Add add, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        void handle(Remove remove, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        void handle(Replace replace, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        void handle(Move move, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        void handle(Copy copy, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        void handle(Increment incr, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model);
        
        default void handle(Require require, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            // do nothing ...
        }
        
        /**
         * 
         * @param op the operation that was performed against the entity; never null
         * @param updatedEntity the updated representation of the entity <em>after</em> the patch was applied; possibly null
         *            if it is not known
         * @param editor the editor for the entity type definition; never null
         * @param entityTypeModel the definition of the entity type; never null
         */
        default void handle(Operation op, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection entityTypeModel) {
            switch (op.action()) {
                case ADD:
                    handle((Add) op, updatedEntity, editor, entityTypeModel);
                    break;
                case REMOVE:
                    handle((Remove) op, updatedEntity, editor, entityTypeModel);
                    break;
                case REPLACE:
                    handle((Replace) op, updatedEntity, editor, entityTypeModel);
                    break;
                case MOVE:
                    handle((Move) op, updatedEntity, editor, entityTypeModel);
                    break;
                case COPY:
                    handle((Copy) op, updatedEntity, editor, entityTypeModel);
                    break;
                case INCREMENT:
                    handle((Increment) op, updatedEntity, editor, entityTypeModel);
                    break;
                case REQUIRE:
                    handle((Require) op, updatedEntity, editor, entityTypeModel);
                    break;
            }
        }
    }
    
    private static FieldType determineBestFieldType(Value value) {
        return FieldType.inferFrom(value).orElse(null);
    }
    
    private static FieldType determineBestFieldType(Value value, Optional<FieldType> knownType) {
        Optional<FieldType> inferred = FieldType.inferFrom(value);
        if (!inferred.isPresent()) return knownType.orElse(null);
        if (!knownType.isPresent()) return inferred.get();
        return knownType.get().union(inferred.get());
    }
    
    protected static class SimpleFieldStrategy implements Strategy {
        @Override
        public void handle(Add add, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            // Make sure that the field is already known and the type can handle the value ...
            Optional<FieldDefinition> field = model.field(add.path());
            if ( field.isPresent() ) {
                // The field exists ...
                Optional<FieldType> knownType = field.get().type();
                FieldType bestType = determineBestFieldType(add.value(), knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    SchemaEditor.editField(editor, add.path()).type(bestType);
                }
            } else {
                // Add the field definition with a best-guess of the type based upon the value ...
                FieldType bestType = determineBestFieldType(add.value());
                SchemaEditor.editField(editor, add.path()).type(bestType);
            }
        }
        
        @Override
        public void handle(Remove remove, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            model.field(remove.path()).ifPresent(field->{
                if (!field.isEmpty() && !field.isOptional()) {
                    // The field is defined, so make sure that it is optional ...
                    SchemaEditor.editField(editor, remove.path()).optional(true);
                }
            });
        }
        
        @Override
        public void handle(Replace replace, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            // The field should already be known, but make sure that the field's type can handle the new value ...
            model.field(replace.path()).ifPresent(field->{
                Optional<FieldType> knownType = field.type();
                FieldType bestType = determineBestFieldType(replace.value(), knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    SchemaEditor.editField(editor, replace.path()).type(bestType);
                }
            });
        }
        
        @Override
        public void handle(Move move, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            // Make sure the 'from' is marked as optional ...
            Optional<FieldDefinition> toField = model.field(move.toPath());
            toField.ifPresent((field)->{
                if ( !field.isOptional() ) {
                    // The field is defined, so make sure that it is optional ...
                    SchemaEditor.editField(editor, move.fromPath()).optional(true);
                }
            });
            
            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.isPresent() ? toField.get().type() : Optional.empty();
            Value value = updatedEntity.get(move.toPath()); // TODO: this might not be the value we were copying
            if ( value != null ) {
                FieldType bestType = determineBestFieldType(value, knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    SchemaEditor.editField(editor, move.toPath()).type(bestType);
                }
            }
        }
        
        @Override
        public void handle(Copy copy, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            Optional<FieldDefinition> toField = model.field(copy.toPath());
            if ( !toField.isPresent() || toField.get().isEmpty() ) {
                // The 'to' field is new to us, but the 'from' field should not be ...
                // Copy the 'from' field definition ...
                SchemaEditor.copyField(editor, copy.fromPath(), copy.toPath());
                return;
            }
            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.get().type();
            Value value = updatedEntity.get(copy.toPath()); // TODO: this might not be the value we were copying
            if ( value != null ) {
                FieldType bestType = determineBestFieldType(value, knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    SchemaEditor.editField(editor, copy.toPath()).type(bestType);
                }
            }
        }
        
        @Override
        public void handle(Increment incr, Document updatedEntity, Patch.Editor<Patch<EntityType>> editor, EntityCollection model) {
            // Make sure that the field is a numeric type ...
        }
    }
    
}
