/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

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
import org.debezium.core.message.Patch.Move;
import org.debezium.core.message.Patch.Operation;
import org.debezium.core.message.Patch.Remove;
import org.debezium.core.message.Patch.Replace;
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
        boolean markAdded(EntityType type, Path fieldPath);

        /**
         * Record the given field as having been removed from an entity.
         * 
         * @param type the entity type; may not be null
         * @param fieldPath the path of the field; may not be null
         * @return true if the field should be considered optional, or false if all entities seen so far have the field
         */
        boolean markRemoved(EntityType type, Path fieldPath);
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
        this.strategy = new ComplexFieldStrategy(new SimpleFieldStrategy(fieldUsage, type));
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
                strategy.remove(Optional.ofNullable(value), path, afterPatch, typeEditor, model);
            });
        } else if (patch.isCreation()) {
            // Process each of the fields in the document as an add ...
            patch.ifCreation(entity -> {
                entity.forEach((path, value) -> {
                    strategy.add(Optional.empty(), path, value, afterPatch, typeEditor, model);
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

        void add(Optional<Value> removedValue, Path pathToField, Value value, Document afterPatch,
                 Patch.Editor<Patch<EntityType>> editor,
                 EntityCollection model);

        void remove(Optional<Value> removedValue, Path pathToField, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                    EntityCollection model);

        void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                  Patch.Editor<Patch<EntityType>> editor,
                  EntityCollection model);

        void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                  Patch.Editor<Patch<EntityType>> editor,
                  EntityCollection model);

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
                    Add add = (Add) op;
                    Path path = Path.parse(add.path());
                    Optional<Value> beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    add(beforeValue, path, add.value(), afterPatch, editor, entityTypeModel);
                    break;
                case REMOVE:
                    Remove remove = (Remove) op;
                    path = Path.parse(remove.path());
                    beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    remove(beforeValue, path, afterPatch, editor, entityTypeModel);
                    break;
                case REPLACE:
                    Replace replace = (Replace) op;
                    path = Path.parse(replace.path());
                    beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    add(beforeValue, path, replace.value(), afterPatch, editor, entityTypeModel);
                    break;
                case MOVE:
                    Move move = (Move) op;
                    Path fromPath = Path.parse(move.fromPath());
                    Path toPath = Path.parse(move.toPath());
                    Optional<Value> movedValue = beforeOp.find(fromPath);
                    Optional<Value> replacedValue = beforeOp.find(toPath);
                    move(fromPath, movedValue, toPath, replacedValue, afterPatch, editor, entityTypeModel);
                    break;
                case COPY:
                    Copy copy = (Copy) op;
                    fromPath = Path.parse(copy.fromPath());
                    toPath = Path.parse(copy.toPath());
                    Optional<Value> copiedValue = beforeOp.find(fromPath);
                    replacedValue = beforeOp.find(toPath);
                    copy(fromPath, copiedValue, toPath, replacedValue, afterPatch, editor, entityTypeModel);
                    break;
                case INCREMENT:
                    break;
                case REQUIRE:
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

    protected static class ComplexFieldStrategy implements Strategy {
        private final Strategy delegate;

        public ComplexFieldStrategy(Strategy delegate) {
            this.delegate = delegate;
        }

        protected void whenComplex(Optional<Value> value, Consumer<Document> function) {
            value.ifPresent(v -> v.ifDocument(function));
        }

        @Override
        public void add(Optional<Value> removedValue, Path pathToField, Value newValue, Document afterPatch,
                        Editor<Patch<EntityType>> editor, EntityCollection model) {
            // Handle complex removed values ...
            if (removedValue.isPresent() && removedValue.get().isDocument()) {
                removedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), pathToField.append(path), afterPatch, editor, model);
                });
                removedValue = Optional.empty();
            }
            if (newValue != null && newValue.isDocument()) {
                if (removedValue.isPresent()) {
                    delegate.remove(removedValue, pathToField, afterPatch, editor, model);
                }
                Document doc = newValue.asDocument();
                // Remove each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.add(Optional.empty(), pathToField.append(path), value, afterPatch, editor, model);
                });
            } else {
                delegate.add(removedValue, pathToField, newValue, afterPatch, editor, model);
            }
        }

        @Override
        public void remove(Optional<Value> removedValue, Path pathToField, Document afterPatch, Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            if (removedValue.isPresent() && removedValue.get().isDocument()) {
                removedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), pathToField.append(path), afterPatch, editor, model);
                });
            } else {
                delegate.remove(removedValue, pathToField, afterPatch, editor, model);
            }
        }

        @Override
        public void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Editor<Patch<EntityType>> editor, EntityCollection model) {
            // If the replaced value is complex, it must be removed piece by piece ...
            if (replacedValue.isPresent() && replacedValue.get().isDocument()) {
                replacedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), toPath.append(path), afterPatch, editor, model);
                });
                replacedValue = Optional.empty();
            }
            // If the copied value is complex, it must be added piece by piece ...
            if (copiedValue.isPresent() && copiedValue.get().isDocument()) {
                if (replacedValue.isPresent()) {
                    // We didn't yet remove the (simple) replaced value ...
                    delegate.remove(replacedValue, toPath, afterPatch, editor, model);
                }
                Document doc = copiedValue.get().asDocument();
                // Add each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.add(Optional.empty(), toPath.append(path), value, afterPatch, editor, model);
                });
            } else {
                // The copied value is not complex, so just delegate ...
                delegate.copy(fromPath, copiedValue, toPath, replacedValue, afterPatch, editor, model);
            }
        }

        @Override
        public void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Editor<Patch<EntityType>> editor, EntityCollection model) {
            // If the replaced value is complex, it must be removed piece by piece ...
            if (replacedValue.isPresent() && replacedValue.get().isDocument()) {
                replacedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), toPath.append(path), afterPatch, editor, model);
                });
                replacedValue = Optional.empty();
            }
            // If the moved value is complex, it must be added piece by piece ...
            if (movedValue.isPresent() && movedValue.get().isDocument()) {
                if (replacedValue.isPresent()) {
                    // We didn't yet remove the (simple) replaced value ...
                    delegate.remove(replacedValue, toPath, afterPatch, editor, model);
                }
                Document doc = movedValue.get().asDocument();
                // Add each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.move(fromPath.append(path), Optional.ofNullable(value), toPath.append(path), Optional.empty(), afterPatch,
                                  editor, model);
                });
                // finally remove the top-level moved value from it's old place ...
                delegate.remove(Optional.of(Value.create(Document.create())), fromPath, afterPatch, editor, model);
            } else {
                // The moved value is not complex, so just delegate ...
                delegate.move(fromPath, movedValue, toPath, replacedValue, afterPatch, editor, model);
            }
        }
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
        public void add(Optional<Value> beforeValue, Path pathToField, Value after, Document afterPatch,
                        Editor<Patch<EntityType>> editor,
                        EntityCollection model) {
            // See if the field is already in the entity before this operation is applied ...
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
            FieldEditor fieldEditor = SchemaEditor.editField(editor, pathToField, model);
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
        public void remove(Optional<Value> beforeValue, Path pathToField, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model) {
            model.field(pathToField).ifPresent(field -> {
                if (!field.isEmpty()) {
                    // The field is defined on the type, so see if the field is in the entity before this operation is applied ...
                    // We only need to do something if the before state had a field to remove ...
                    beforeValue.ifPresent(value -> {
                        // Figure out if we have to change the optional attribute of the field ...
                        boolean optional = fieldUsage.markRemoved(type, pathToField);
                        if (optional && !field.isOptional()) {
                            SchemaEditor.editField(editor, pathToField).optional(true);
                        } else if (!optional && field.isOptional()) {
                            SchemaEditor.editField(editor, pathToField).optional(false);
                        }
                    });
                }
            });
        }

        @Override
        public void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Patch.Editor<Patch<EntityType>> editor,
                         EntityCollection model) {
            Optional<Boolean> fromIsOptional = Optional.empty();
            boolean movedValueExists = movedValue.isPresent() && !Value.isNull(movedValue.get());
            if (movedValueExists) {
                // There is a value being (re)moved ...
                fromIsOptional = Optional.of(fieldUsage.markRemoved(type, fromPath));
            }

            // Make sure the 'from' is marked as optional ...
            Optional<FieldDefinition> toField = model.field(toPath);
            Optional<FieldDefinition> fromField = model.field(fromPath);
            if (fromField.isPresent()) {
                FieldDefinition field = fromField.get();
                if (fromIsOptional.isPresent() && field.isOptional() != fromIsOptional.get()) {
                    // The field is defined, so make sure that it is optional ...
                    SchemaEditor.editField(editor, fromPath).optional(fromIsOptional.get());
                }
                if (!toField.isPresent()) {
                    // Copy the field definition ...
                    SchemaEditor.copyField(editor, fromPath, toPath);
                }
            }

            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.isPresent() ? toField.get().type() : Optional.empty();
            FieldEditor fieldEditor = SchemaEditor.editField(editor, toPath, model);
            if (movedValueExists) {
                FieldType bestType = determineBestFieldType(movedValue.get(), knownType);
                if (!toField.isPresent() || knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    fieldEditor.type(bestType);
                }
                boolean toIsOptional = fieldUsage.markAdded(type, toPath);
                if (toField.isPresent()) {
                    if (toField.get().isOptional() != toIsOptional) fieldEditor.optional(toIsOptional);
                } else {
                    fieldEditor.optional(toIsOptional);
                }
            } else {
                // The new value is null ...
                replacedValue.ifPresent(oldValue -> {
                    if (Value.notNull(oldValue)) {
                        boolean toIsOptional = fieldUsage.markRemoved(type, toPath);
                        toField.ifPresent(field -> {
                            if (field.isOptional() != toIsOptional) fieldEditor.optional(toIsOptional);
                        });
                    }
                });
            }
        }

        @Override
        public void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Patch.Editor<Patch<EntityType>> editor,
                         EntityCollection model) {
            Optional<FieldDefinition> toField = model.field(toPath.toRelativePath());
            if (!toField.isPresent() || toField.get().isEmpty()) {
                // The 'to' field is new to us, but the 'from' field should not be ...
                // Copy the 'from' field definition ...
                SchemaEditor.copyField(editor, fromPath, toPath);
            }
            // The 'to' field either already exists, or is brand new (and the 'from' does not exist).
            // Either way, just make sure the type can handle the value ...
            Optional<FieldType> knownType = toField.get().type();
            FieldEditor fieldEditor = SchemaEditor.editField(editor, toPath, model);
            if (copiedValue.isPresent()) {
                FieldType bestType = determineBestFieldType(copiedValue.get(), knownType);
                if (!toField.isPresent() || knownType.orElse(bestType) != bestType) {
                    // We have to change the type ...
                    fieldEditor.type(bestType);
                }
                boolean toIsOptional = fieldUsage.markAdded(type, toPath);
                if (toField.isPresent()) {
                    if (toField.get().isOptional() != toIsOptional) fieldEditor.optional(toIsOptional);
                } else {
                    fieldEditor.optional(toIsOptional);
                }
            } else {
                // The copied value is null ...
                replacedValue.ifPresent(oldValue -> {
                    if (Value.notNull(oldValue)) {
                        boolean toIsOptional = fieldUsage.markRemoved(type, toPath);
                        toField.ifPresent(field -> {
                            if (field.isOptional() != toIsOptional) fieldEditor.optional(toIsOptional);
                        });
                    }
                });
            }
        }
    }

}
