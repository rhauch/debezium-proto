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
import org.debezium.core.component.SchemaEditor.FieldEditor;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Path;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Add;
import org.debezium.core.message.Patch.Copy;
import org.debezium.core.message.Patch.Editor;
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

    public static interface FieldUsage {

        /**
         * Record the creation of a new entity for the given type.
         * 
         * @param type the entity type; may not be null
         */
        void markNewEntity(EntityType type);

        /**
         * Record the given field as having been added to an entity.
         * 
         * @param type the entity type; may not be null
         * @param fieldPath the path of the field; may not be null
         * @return true if the field should be considered optional, or false if all entities seen so far have the field
         */
        boolean markAdded(EntityType type, String fieldPath);

        /**
         * Record the given field as having been removed from an entity.
         * 
         * @param type the entity type; may not be null
         * @param fieldPath the path of the field; may not be null
         * @return true if the field should be considered optional, or false if all entities seen so far have the field
         */
        boolean markRemoved(EntityType type, String fieldPath);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LearningEntityTypeModel.class);

    private final EntityType type;
    private final FieldUsage fieldUsage;
    private EntityCollection model;
    private Patch.Editor<Patch<EntityType>> typeEditor;
    private Strategy strategy;

    /**
     * Create a new model for the given entity type and starting representation.
     * 
     * @param type the entity type; may not be null
     * @param model the starting representation of the entity type; may not be null
     * @param fieldUsage the FieldUsage implementation; may not be null
     */
    public LearningEntityTypeModel(EntityType type, Document model, FieldUsage fieldUsage) {
        this.type = type;
        this.fieldUsage = fieldUsage;
        this.model = EntityCollection.with(type, model);
        this.typeEditor = Patch.edit(type);
        this.strategy = new SimpleFieldStrategy(fieldUsage, type);
    }

    /**
     * Update this model based upon the given entity patch and complete representation.
     * 
     * @param beforePatch the representation of the entity <em>before</em> the patch was applied; may be null if the patch creates
     *            the entity
     * @param patch the patch that was successfully applied to the entity; never null
     * @param afterPatch the updated representation of the entity <em>after</em> the patch was applied
     * @param updatedEntityType the function that should be called if these updates alter the entity type model; may be null
     */
    public void adapt(Document beforePatch, Patch<EntityId> patch, Document afterPatch, Consumer<Patch<EntityType>> updatedEntityType) {
        // Create a working state of the entity that starts with the 'beforePatch' state and that we'll update with each patch
        // operation. This will allow us to know the before and after of each operation...
        Document working = beforePatch != null ? beforePatch : Document.create();
        if (beforePatch == null) fieldUsage.markNewEntity(type);
        // Adapt to the supplied entity patch ...
        if (patch.isDeletion()) {
            // Process each of the fields in the document as a removal ...
            beforePatch.forEach((path, value) -> {
                strategy.remove(beforePatch, path.toString(), afterPatch, typeEditor, model);
            });
        } else if ( patch.isCreation() ) {
            // Process each of the fields in the document as an add ...
            patch.ifCreation(entity->{
                entity.forEach((path, value) -> {
                    String relativePath = path.toRelativePath();
                    strategy.add(beforePatch, relativePath, value, afterPatch, typeEditor, model);
                });
            });
        } else {
            // Process each of the operations ...
            patch.forEach(op -> {
                // First have the strategy adapt to the operation ...
                strategy.handle(beforePatch, op, afterPatch, typeEditor, model);
                // Then update the working state of the entity by applying the patch ...
                op.apply(working, failedPath -> LOGGER.error("Unable to apply {} to entity {}: {}", op, patch.target(), working));
            });
        }
        // Now figure out if anything changed ...
        typeEditor.endIfChanged().ifPresent((entityTypePatch) -> {
            // Update the model ...
            entityTypePatch.apply(model.document(),
                                  failedOp -> LOGGER.error("Unable to apply {} to model for {}: {}", failedOp, type, model));
            // And call the supplied function ...
            updatedEntityType.accept(entityTypePatch);
            typeEditor = Patch.edit(type);
        });
    }

    protected static interface Strategy {

        void add(Document beforeOp, String pathToField, Value value, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                 EntityCollection model);

        void remove(Document beforeOp, String pathToField, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                    EntityCollection model);

        default void handle(Document beforeOp, Add add, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            EntityCollection model) {
            add(beforeOp, add.path(), add.value(), afterPatch, editor, model);
        }

        default void handle(Document beforeOp, Remove remove, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            EntityCollection model) {
            remove(beforeOp, remove.path(), afterPatch, editor, model);
        }

        default void handle(Document beforeOp, Replace replace, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            EntityCollection model) {
            add(beforeOp, replace.path(), replace.value(), afterPatch, editor, model);
        }

        void handle(Document beforeOp, Move move, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                    EntityCollection model);

        void handle(Document beforeOp, Copy copy, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                    EntityCollection model);

        void handle(Document beforeOp, Increment incr, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                    EntityCollection model);

        default void handle(Document beforeOp, Require require, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            boolean isFirst, EntityCollection model) {
            // do nothing ...
        }

        /**
         * 
         * @param beforeOp the representation of the entity <em>before</em> the patch operation is applied; may be null if the
         *            patch creates the entity
         * @param op the operation that was performed against the entity; never null
         * @param afterPatch the updated representation of the entity <em>after</em> the patch was applied
         * @param editor the editor for the entity type definition; never null
         * @param entityTypeModel the definition of the entity type; never null
         */
        default void handle(Document beforeOp, Operation op, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            EntityCollection entityTypeModel) {
            switch (op.action()) {
                case ADD:
                    handle(beforeOp, (Add) op, afterPatch, editor, entityTypeModel);
                    break;
                case REMOVE:
                    handle(beforeOp, (Remove) op, afterPatch, editor, entityTypeModel);
                    break;
                case REPLACE:
                    handle(beforeOp, (Replace) op, afterPatch, editor, entityTypeModel);
                    break;
                case MOVE:
                    handle(beforeOp, (Move) op, afterPatch, editor, entityTypeModel);
                    break;
                case COPY:
                    handle(beforeOp, (Copy) op, afterPatch, editor, entityTypeModel);
                    break;
                case INCREMENT:
                    handle(beforeOp, (Increment) op, afterPatch, editor, entityTypeModel);
                    break;
                case REQUIRE:
                    handle(beforeOp, op, afterPatch, editor, entityTypeModel);
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
        private final FieldUsage fieldUsage;
        private final EntityType type;

        public SimpleFieldStrategy(FieldUsage fieldUsage, EntityType type) {
            this.fieldUsage = fieldUsage;
            this.type = type;
            assert this.fieldUsage != null;
            assert this.type != null;
        }

        @Override
        public void add(Document before, String pathToField, Value after, Document afterPatch, Editor<Patch<EntityType>> editor,
                        EntityCollection model) {
            // See if the field is already in the entity before this operation is applied ...
            Path path = Path.parse(pathToField);
            Optional<Value> beforeValue = before != null ? before.find(path) : Optional.empty();
            Optional<Boolean> shouldBeOptional = Optional.empty();
            if (beforeValue.isPresent()) {
                // There is a before value ...
                if (beforeValue.equals(after)) {
                    // The before and after values are the same, so there's nothing to do ...
                    return;
                }
                if (Value.isNull(after)) {
                    // There is no longer a value after, which means we're removing it ...
                    shouldBeOptional = Optional.of(fieldUsage.markRemoved(type, pathToField));
                }
            } else {
                // There is no before value ...
                if (Value.isNull(after)) {
                    // The before and after values are both null, so there's nothing to do ...
                    return;
                }
                // There is now a value after, which means we're adding it ...
                shouldBeOptional = Optional.of(fieldUsage.markAdded(type, pathToField));
            }

            // Make sure that the field is already known and the type can handle the value ...
            Optional<FieldDefinition> field = model.field(pathToField);
            FieldEditor fieldEditor = SchemaEditor.editField(editor, path);
            if (field.isPresent()) {
                // The field exists ...
                Optional<FieldType> knownType = field.get().type();
                FieldType bestType = determineBestFieldType(after, knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    fieldEditor.type(bestType);
                }
                shouldBeOptional.ifPresent(expectedOptionalSetting -> {
                    // Set the field's optional attribute only if it doesn't match the expected optional setting ...
                    if (field.get().isOptional() != expectedOptionalSetting) fieldEditor.optional(expectedOptionalSetting);
                });
            } else {
                // Add the field definition with a best-guess of the type based upon the value ...
                FieldType bestType = determineBestFieldType(after);
                fieldEditor.type(bestType);
                shouldBeOptional.ifPresent(fieldEditor::optional);
            }
        }

        @Override
        public void remove(Document beforeOp, String pathToField, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            model.field(pathToField).ifPresent(field -> {
                if (!field.isEmpty()) {
                    // The field is defined on the type, so see if the field is in the entity before this operation is applied ...
                    Path path = Path.parse(pathToField);
                    Optional<Value> beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    // We only need to do something if the before state had a field to remove ...
                    beforeValue.ifPresent(value -> {
                        // Figure out if we have to change the optional attribute of the field ...
                        boolean optional = fieldUsage.markRemoved(type, pathToField);
                        if (optional && !field.isOptional()) {
                            SchemaEditor.editField(editor, path).optional(true);
                        } else if (!optional && field.isOptional()) {
                            SchemaEditor.editField(editor, path).optional(false);
                        }
                    });
                }
            });
        }

        @Override
        public void handle(Document beforeOp, Move move, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            Value movedValue = beforeOp.get(move.fromPath());
            Value replacedValue = beforeOp.get(move.toPath());
            Optional<Boolean> fromIsOptional = Optional.empty();
            if (!Value.isNull(movedValue)) {
                // There is a value being (re)moved ...
                fromIsOptional = Optional.of(fieldUsage.markRemoved(type, move.fromPath()));
            }

            // Make sure the 'from' is marked as optional ...
            Optional<FieldDefinition> toField = model.field(move.toPath());
            Optional<FieldDefinition> fromField = model.field(move.fromPath());
            if ( fromField.isPresent() ) {
                FieldDefinition field = fromField.get();
                if (fromIsOptional.isPresent() && field.isOptional() != fromIsOptional.get() ) {
                    // The field is defined, so make sure that it is optional ...
                    SchemaEditor.editField(editor, move.fromPath()).optional(fromIsOptional.get());
                }
            }

            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.isPresent() ? toField.get().type() : Optional.empty();
            FieldEditor fieldEditor = SchemaEditor.editField(editor, move.toPath());
            if (movedValue != null) {
                FieldType bestType = determineBestFieldType(movedValue, knownType);
                if (!toField.isPresent() || knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    fieldEditor.type(bestType);
                }
                boolean toIsOptional = fieldUsage.markAdded(type, move.toPath());
                if ( toField.isPresent() ) {
                    if ( toField.get().isOptional() != toIsOptional ) fieldEditor.optional(toIsOptional);
                } else {
                    fieldEditor.optional(toIsOptional);
                }
            } else {
                // If there was a value being replaced with a null value, we have to remove it ...
                if ( Value.isNull(replacedValue) ) {
                    boolean toIsOptional = fieldUsage.markRemoved(type, move.toPath());
                    toField.ifPresent(field->{
                        if ( field.isOptional() != toIsOptional ) fieldEditor.optional(toIsOptional);
                    });
                }
            }
        }

        @Override
        public void handle(Document beforeOp, Copy copy, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            Optional<FieldDefinition> toField = model.field(copy.toPath());
            if (!toField.isPresent() || toField.get().isEmpty()) {
                // The 'to' field is new to us, but the 'from' field should not be ...
                // Copy the 'from' field definition ...
                SchemaEditor.copyField(editor, copy.fromPath(), copy.toPath());
                return;
            }
            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.get().type();
            Value value = beforeOp.get(copy.fromPath());
            if (value != null) {
                FieldType bestType = determineBestFieldType(value, knownType);
                if (knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    SchemaEditor.editField(editor, copy.toPath()).type(bestType);
                }
            }
        }

        @Override
        public void handle(Document beforeOp, Increment incr, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            // Make sure that the field is a numeric type ...
        }
    }

}
