/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.debezium.annotation.NotThreadSafe;
import org.debezium.crdt.CRDT;
import org.debezium.crdt.Count;
import org.debezium.crdt.DeltaCount;
import org.debezium.crdt.DeltaCounter;
import org.debezium.message.Document;
import org.debezium.message.Message.Field;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Add;
import org.debezium.message.Patch.Copy;
import org.debezium.message.Patch.Editor;
import org.debezium.message.Patch.Move;
import org.debezium.message.Patch.Operation;
import org.debezium.message.Patch.Remove;
import org.debezium.message.Patch.Replace;
import org.debezium.message.Path;
import org.debezium.message.Value;
import org.debezium.model.EntityCollection.FieldDefinition;
import org.debezium.model.EntityCollection.FieldName;
import org.debezium.model.EntityCollection.FieldType;
import org.debezium.model.SchemaEditor.FieldEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A model of an entity type that learns its structure based upon individual changes to entities.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class EntityTypeLearningModel {

    /**
     * Metrics for the number of entities and numbers of values of each field.
     */
    @NotThreadSafe
    public static interface Metrics {
        /**
         * Get the number of entities.
         * 
         * @return the total number of entities that produced this model
         */
        DeltaCount getEntityCount();

        /**
         * Get the paths to each field in the entity.
         * 
         * @return the field paths; never null
         */
        Set<Path> fieldPaths();

        /**
         * Get the number of entities that have the field given by the supplied path.
         * 
         * @param path the path of the field in the entities; may not be null
         * @return the counter for the number of entities that have the named field; null if there is no path
         */
        DeltaCount getFieldCount(Path path);

        /**
         * For each of the fields in the entity, call the given consumer.
         * 
         * @param consumer the consumer; may not be null
         */
        void forEachField(BiConsumer<Path, DeltaCount> consumer);

        /**
         * Get the document representation of the metrics.
         * 
         * @return the representation; never null
         */
        Document asDocument();

        /**
         * Merge the specified metrics into this instance.
         * 
         * @param other the other metrics
         * @return this object so methods can be chained together; never null
         */
        Metrics merge(Metrics other);

        /**
         * Determine if there are any changes in this count.
         * @return {@code true} if there are non-zero changes in the total or fields, or {@code false} otherwise
         */
        boolean hasChanges();
    }

    /**
     * Interface describing the field changes for a specific {@link EntityType}.
     */
    protected static interface FieldUsage {

        /**
         * Record the creation of a new entity for the given type.
         * 
         * @return the updated count showing the total number of entities; never null
         */
        DeltaCount markNewEntity();

        /**
         * Record the creation of a new entity for the given type.
         * 
         * @return the updated count showing the total number of entities; never null
         */
        DeltaCount markRemovedEntity();

        /**
         * Record the given field as having been added to an entity.
         * 
         * @param fieldPath the path of the field; may not be null
         * @return the updated count showing the occurrences of this field; never null
         */
        DeltaCount markAdded(Path fieldPath);

        /**
         * Record the given field as having been removed from an entity.
         * 
         * @param fieldPath the path of the field; may not be null
         * @return the updated count showing the occurrences of this field; never null
         */
        DeltaCount markRemoved(Path fieldPath);
    }

    protected static class FieldMetrics implements FieldUsage, Metrics {
        private final ConcurrentMap<Path, DeltaCounter> counts = new ConcurrentHashMap<>();
        private final DeltaCounter entityCount;

        public FieldMetrics(Document doc) {
            entityCount = doc == null ? CRDT.newDeltaCounter() : CRDT.newDeltaCounter(doc.getLong("totalAdds", 0L),
                                                                                      doc.getLong("totalRemoves", 0L),
                                                                                      doc.getLong("recentAdds", 0L),
                                                                                      doc.getLong("recentRemoves", 0L));
            if (doc != null) {
                Document fields = doc.getDocument("fields");
                if (fields != null) {
                    fields.forEach(field -> {
                        Path path = Path.parse(field.getName().toString());
                        Document nested = field.getValue().asDocument();
                        DeltaCounter counter = CRDT.newDeltaCounter(nested.getLong("totalAdds", 0L),
                                                                    nested.getLong("totalRemoves", 0L),
                                                                    nested.getLong("recentAdds", 0L),
                                                                    nested.getLong("recentRemoves", 0L));
                        counts.put(path, counter);
                    });
                }
            }
        }

        @Override
        public Document asDocument() {
            Document doc = Document.create();
            doc.setNumber("totalAdds", entityCount.getIncrement());
            doc.setNumber("totalRemoves", entityCount.getDecrement());
            doc.setNumber("recentAdds", entityCount.getChanges().getIncrement());
            doc.setNumber("recentRemoves", entityCount.getChanges().getDecrement());
            Document fields = doc.setDocument("fields");
            counts.forEach((path, counter) -> {
                Document nested = Document.create();
                nested.setNumber("totalAdds", counter.getIncrement());
                nested.setNumber("totalRemoves", counter.getDecrement());
                nested.setNumber("recentAdds", counter.getChanges().getIncrement());
                nested.setNumber("recentRemoves", counter.getChanges().getDecrement());
                fields.setDocument(path.toRelativePath(),nested);
            });
            return doc;
        }

        @Override
        public Metrics merge(Metrics other) {
            this.entityCount.merge(other.getEntityCount());
            // Go through all of this metrics' fields ...
            this.counts.forEach((path, counter) -> {
                // Find the field in the other and merge the **changes** in the field counts ...
                DeltaCount otherCount = other.getFieldCount(path);
                if (otherCount != null) counter.merge(otherCount);
            });
            // Get the new fields (if any) in other ...
            Set<Path> newPaths = new HashSet<>(other.fieldPaths());
            newPaths.removeAll(this.fieldPaths());
            if (!newPaths.isEmpty()) {
                newPaths.forEach(newPath -> {
                    DeltaCounter counter = CRDT.newDeltaCounter(other.getFieldCount(newPath));
                    this.counts.put(newPath, counter);
                });
            }
            return this;
        }

        @Override
        public DeltaCount markNewEntity() {
            return entityCount.increment();
        }

        @Override
        public DeltaCount markRemovedEntity() {
            return entityCount.decrement();
        }

        @Override
        public DeltaCounter markAdded(Path fieldPath) {
            return counts.computeIfAbsent(fieldPath, key -> CRDT.newDeltaCounter()).increment();
        }

        @Override
        public DeltaCounter markRemoved(Path fieldPath) {
            return counts.computeIfAbsent(fieldPath, key -> CRDT.newDeltaCounter()).decrement();
        }

        @Override
        public DeltaCount getEntityCount() {
            return entityCount;
        }

        @Override
        public void forEachField(BiConsumer<Path, DeltaCount> consumer) {
            counts.forEach((path, count) -> consumer.accept(path, count));
        }

        @Override
        public Set<Path> fieldPaths() {
            return new HashSet<Path>(counts.keySet());
        }

        @Override
        public DeltaCount getFieldCount(Path path) {
            return counts.get(path);
        }
        
        @Override
        public boolean hasChanges() {
            if ( entityCount.hasChanges()) return true;
            return counts.values().stream().anyMatch(DeltaCounter::hasChanges);
        }

        public void resetChanges() {
            entityCount.reset();
            counts.forEach((path, count) -> count.reset());
        }

        @Override
        public String toString() {
            return asDocument().toString();
        }
    }

    /**
     * Response function called in response to a change in a model when
     * {@link EntityTypeLearningModel#adapt(Document, Patch, Document, long, boolean, Adaptation)} is called.
     */
    @FunctionalInterface
    public static interface Adaptation {
        /**
         * Respond to the model being modified due to a patch to an entity
         * 
         * @param before the snapshot of the model before the patch was applied; may be null
         * @param timestamp the timestamp of the previous adaptation
         * @param updatedEntityType the patch that describes the change to the model; never null
         */
        public void adapted(Document before, long timestamp, Patch<EntityType> updatedEntityType);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityTypeLearningModel.class);

    private final EntityType type;
    private final AtomicLong timestamp = new AtomicLong();
    private final AtomicLong revision = new AtomicLong();
    private EntityCollection model;
    private Patch.Editor<Patch<EntityType>> typeEditor;
    private Strategy strategy;
    private final FieldMetrics metrics;
    private final Document document;

    /**
     * Create a new model for the given entity type and starting representation.
     * 
     * @param type the entity type; may not be null
     * @param model the starting representation of the entity type; may not be null
     */
    public EntityTypeLearningModel(EntityType type, Document model) {
        this.type = type;
        this.document = model;
        this.model = EntityCollection.with(type, this.document);
        this.timestamp.set(this.document.getLong(Field.ENDED, 0L));
        this.revision.set(this.document.getLong(Field.REVISION, 1L));
        this.metrics = new FieldMetrics(this.document.getDocument(Field.METRICS));
        this.typeEditor = Patch.edit(type);
        this.strategy = new ComplexFieldStrategy(new SimpleFieldStrategy(this.metrics));
    }

    /**
     * Get the document representation of this entity type model, including metrics.
     * 
     * @return the representation of this model; never null
     */
    public Document asDocument() {
        return asDocument(true);
    }

    /**
     * Get the document representation of this entity type model, possibly including metrics.
     * 
     * @param includeMetrics {@code true} if the metrics should be included in the document, in which case the actual underlying
     *            document is returned, or {@code false} if the metrics should be excluded from the document and a clone of this
     *            document
     *            is returned
     * @return the representation of this model; never null
     */
    public Document asDocument(boolean includeMetrics) {
        if (this.document.isEmpty()) return null;
        this.document.setDocument(Field.METRICS, metrics.asDocument());
        this.document.setNumber(Field.ENDED, timestamp.get());
        this.document.setNumber(Field.REVISION, revision.get());
        if (!includeMetrics) {
            Document result = this.document.clone();
            result.remove(Field.METRICS);
            return result;
        }
        return this.document;
    }

    /**
     * Get the EntityType for this model.
     * 
     * @return the type; never null
     */
    public EntityType type() {
        return type;
    }

    /**
     * Get the metrics associated with this model.
     * 
     * @return the metrics; never null
     */
    public Metrics metrics() {
        return this.metrics;
    }

    /**
     * Reset any of the metrics that track recent changes. This does not affect total metrics.
     * 
     * @return this model so that methods can be chained together; never null
     */
    public EntityTypeLearningModel resetChangeStatistics() {
        this.metrics.resetChanges();
        return this;
    }

    /**
     * Get the timestamp that this model was last modified.
     * 
     * @return the timestamp of the last modification
     */
    public long lastModified() {
        return timestamp.get();
    }

    /**
     * Get the revision number for this model.
     * 
     * @return the revision number
     */
    public long revision() {
        return revision.get();
    }

    /**
     * Merge into this model the changes described by the supplied patch and differential metrics.
     * 
     * @param metrics the incoming metrics with changes that should be merged into this model's metrics; may be null
     * @param entityTypePatch the patch that should be applied to the underlying EntityType model; may be null
     * @param timestamp the current timestamp that this change is being made
     * @param failureHandler the function that is called for each failed operation because the path did not exist; may be null
     * @return {@code true} if the model was updated by the patch, or {@code false} if no changes were made to the model (although
     *         metrics might have been updated)
     */
    public boolean merge(Metrics metrics, Patch<EntityType> entityTypePatch, long timestamp, BiConsumer<Path, Operation> failureHandler) {
        // Apply the patch to this model ...
        boolean changed = entityTypePatch == null ? false : entityTypePatch.apply(this.document, failureHandler);
        // Then update the metrics ...
        merge(metrics);
        if (changed) {
            this.revision.incrementAndGet();
            this.timestamp.set(timestamp);
        }
        return changed;
    }

    /**
     * Merge into this model the changes described by the supplied differential metrics.
     * 
     * @param metrics the incoming metrics with changes that should be merged into this model's metrics; may be null
     * @return this model so that methods can be chained together; never null
     */
    public EntityTypeLearningModel merge(Metrics metrics) {
        // Merge the *changes* from the supplied metrics ...
        if (metrics != null) {
            this.metrics.merge(metrics);
            // Update the field usage ...
            updateFieldUsages();
        }
        return this;
    }

    /**
     * Update this model based upon the given entity patch and complete representation.
     * 
     * @param beforePatch the representation of the entity <em>before</em> the patch was applied; may be null if the patch creates
     *            the entity
     * @param patch the patch that was successfully applied to the entity; never null
     * @param afterPatch the updated representation of the entity <em>after</em> the patch was applied
     * @param timestamp the current timestamp that this change is being made
     * @param includeUsageUpdates {@code true} if {@link FieldName#USAGE} updates should result in patch changes, or {@code false}
     * if {@link FieldName#USAGE} updates alone should not result in a patch change to the entity type
     * @param updatedEntityType the function that should be called if these updates alter the entity type model; may be null
     */
    public void adapt(Document beforePatch, Patch<EntityId> patch, Document afterPatch, long timestamp, boolean includeUsageUpdates,
                      Adaptation updatedEntityType) {
        // Create a working state of the entity that starts with the 'beforePatch' state and that we'll update with each patch
        // operation. This will allow us to know the before and after of each operation...
        Document working = beforePatch != null ? beforePatch : Document.create();
        if (beforePatch == null) this.metrics.markNewEntity();
        // Adapt to the supplied entity patch ...
        if (patch.isDeletion()) {
            // Process each of the fields in the document as a removal ...
            DeltaCount entityCount = this.metrics.markRemovedEntity();
            beforePatch.forEach((path, value) -> {
                strategy.remove(Optional.ofNullable(value), path, afterPatch, typeEditor, model, entityCount);
            });
        } else if (patch.isCreation()) {
            // Process each of the fields in the document as an add ...
            patch.ifCreation(entity -> {
                entity.forEach((path, value) -> {
                    strategy.add(Optional.empty(), path, value, afterPatch, typeEditor, model, metrics.getEntityCount());
                });
            });
        } else {
            // Process each of the operations ..
            patch.forEach(op -> {
                // First have the strategy adapt to the operation ...
                strategy.handle(beforePatch, op, afterPatch, typeEditor, model, metrics.getEntityCount());
                // Then update the working state of the entity by applying the patch ...
                op.apply(working, failedPath -> LOGGER.error("Unable to apply {} to entity {}: {}", op, patch.target(), working));
            });
        }
        // Now figure out if anything changed ...
        recalculateFieldUsages();
        typeEditor.endIfChanged(op->includeUsageUpdates ? true : withSignifantOperations(op)).ifPresent((entityTypePatch) -> {
            Document before = model.document().clone();
            // Update the model ...
            entityTypePatch.apply(model.document(),
                                  (failedPath, failedOp) -> LOGGER.error("Unable to apply {} to model for {}: {}", failedOp, type,
                                                                         model));
            // And call the supplied function ...
            updatedEntityType.adapted(before, this.timestamp.getAndSet(timestamp), entityTypePatch);
            revision.incrementAndGet();
        });
        typeEditor = Patch.edit(type);
    }

    protected boolean withSignifantOperations(Operation operation) {
        // If this is replacing the "usage" field, it is not significant ...
        if (operation instanceof Replace) {
            Replace replace = (Replace) operation;
            if (replace.path().endsWith("/" + EntityCollection.FieldName.USAGE)) return false;
        }
        return true;
    }

    /**
     * Determine if this model has any recorded changes, including changes in statistics, and if so return the patch describing
     * those changes.
     * 
     * @param timestamp the timestamp of the modification
     * @return the optional patch; never null but possibly {@link Optional#empty() empty}
     */
    public Optional<Patch<EntityType>> getPatch(long timestamp) {
        Optional<Patch<EntityType>> result = typeEditor.endIfChanged();
        typeEditor = Patch.edit(type);
        this.timestamp.set(timestamp);
        return result;
    }

    /**
     * Forcibly recalculate each field's usage metric and update the model.
     */
    protected void updateFieldUsages() {
        DeltaCount totalCount = metrics.getEntityCount();
        Document fields = this.document.getDocument("fields");
        model.fields().forEach(field -> {
            Path pathToField = Path.parse(field.name());
            DeltaCount fieldCount = metrics.getFieldCount(pathToField);
            if (fieldCount != null) {
                // Could use a new editor, but this is faster ...
                fields.getDocument(field.name()).setNumber(FieldName.USAGE, computeUsage(fieldCount, totalCount));
            }
        });
    }

    /**
     * Forcibly recalculate each field's usage metric and update the model via the editor.
     * 
     * @return this model so methods can be chained together; never null
     */
    public EntityTypeLearningModel recalculateFieldUsages() {
        DeltaCount totalCount = metrics.getEntityCount();
        model.fields().forEach(field -> {
            Path pathToField = Path.parse(field.name());
            DeltaCount fieldCount = metrics.getFieldCount(pathToField);
            if (fieldCount != null) {
                setUsageIfNeeded(field, fieldCount, totalCount, () -> SchemaEditor.editField(typeEditor, pathToField, model));
            }
        });
        return this;
    }

    private static float computeUsage(Count fieldCount, Count totalCount) {
        float fCount = fieldCount.get();
        float tCount = totalCount.get();
        return tCount == 0 ? 0 : fCount / tCount;
    }

    protected static void setUsageIfNeeded(FieldDefinition field, DeltaCount fieldCount, DeltaCount total, Supplier<FieldEditor> fieldEditor) {
        if (field != null) {
            // Determine the current usage, and compute the expected usage ...
            float oldUsage = computeUsage(fieldCount.getPriorCount(), total.getPriorCount());
            float newUsage = computeUsage(fieldCount, total);
            if (Math.abs(oldUsage - newUsage) > 0.0000001) {
                // They're different, so we have to update the field ...
                fieldEditor.get().usage(fieldCount, total);
            }
        } else {
            // Always set it ...
            fieldEditor.get().usage(fieldCount, total);
        }
    }
    
    /**
     * Recreate the "before" representation of this entity type model as it was before any of the recently-recorded metric
     * changes.
     * @return the "before" representation; never null
     * @see #asDocument()
     * @see #asDocument(boolean)
     */
    public Document recreateDocumentBeforeRecentUsageChanges() {
        Document before = this.asDocument(false);   // clone
        Count totalCount = metrics.getEntityCount().getPriorCount();
        Document fields = before.getDocument("fields");
        model.fields().forEach(field -> {
            Path pathToField = Path.parse(field.name());
            Count fieldCount = metrics.getFieldCount(pathToField).getPriorCount();
            fields.getDocument(field.name()).setNumber(FieldName.USAGE, computeUsage(fieldCount,totalCount));
        });
        before.setNumber(Field.REVISION, revision.get()-1);
        return before;
    }

    protected static interface Strategy {

        void add(Optional<Value> removedValue, Path pathToField, Value value, Document afterPatch,
                 Patch.Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount);

        void remove(Optional<Value> removedValue, Path pathToField, Document afterPatch,
                    Patch.Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount);

        void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                  Patch.Editor<Patch<EntityType>> editor,
                  EntityCollection model, DeltaCount entityCount);

        void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                  Patch.Editor<Patch<EntityType>> editor,
                  EntityCollection model, DeltaCount entityCount);

        /**
         * 
         * @param beforeOp the representation of the entity <em>before</em> the patch operation is applied; may be null if the
         *            patch creates the entity
         * @param op the operation that was performed against the entity; never null
         * @param afterPatch the updated representation of the entity <em>after</em> the patch was applied
         * @param editor the editor for the entity type definition; never null
         * @param entityTypeModel the definition of the entity type; never null
         * @param entityCount the total number of entities; may not be null
         */
        default void handle(Document beforeOp, Operation op, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                            EntityCollection entityTypeModel, DeltaCount entityCount) {
            switch (op.action()) {
                case ADD:
                    Add add = (Add) op;
                    Path path = Path.parse(add.path());
                    Optional<Value> beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    add(beforeValue, path, add.value(), afterPatch, editor, entityTypeModel, entityCount);
                    break;
                case REMOVE:
                    Remove remove = (Remove) op;
                    path = Path.parse(remove.path());
                    beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    remove(beforeValue, path, afterPatch, editor, entityTypeModel, entityCount);
                    break;
                case REPLACE:
                    Replace replace = (Replace) op;
                    path = Path.parse(replace.path());
                    beforeValue = beforeOp != null ? beforeOp.find(path) : Optional.empty();
                    add(beforeValue, path, replace.value(), afterPatch, editor, entityTypeModel, entityCount);
                    break;
                case MOVE:
                    Move move = (Move) op;
                    Path fromPath = Path.parse(move.fromPath());
                    Path toPath = Path.parse(move.toPath());
                    Optional<Value> movedValue = beforeOp.find(fromPath);
                    Optional<Value> replacedValue = beforeOp.find(toPath);
                    move(fromPath, movedValue, toPath, replacedValue, afterPatch, editor, entityTypeModel, entityCount);
                    break;
                case COPY:
                    Copy copy = (Copy) op;
                    fromPath = Path.parse(copy.fromPath());
                    toPath = Path.parse(copy.toPath());
                    Optional<Value> copiedValue = beforeOp.find(fromPath);
                    replacedValue = beforeOp.find(toPath);
                    copy(fromPath, copiedValue, toPath, replacedValue, afterPatch, editor, entityTypeModel, entityCount);
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
                        Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount) {
            // Handle complex removed values ...
            if (removedValue.isPresent() && removedValue.get().isDocument()) {
                removedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), pathToField.append(path), afterPatch, editor, model, entityCount);
                });
                removedValue = Optional.empty();
            }
            if (newValue != null && newValue.isDocument()) {
                if (removedValue.isPresent()) {
                    delegate.remove(removedValue, pathToField, afterPatch, editor, model, entityCount);
                }
                Document doc = newValue.asDocument();
                // Remove each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.add(Optional.empty(), pathToField.append(path), value, afterPatch, editor, model, entityCount);
                });
            } else {
                delegate.add(removedValue, pathToField, newValue, afterPatch, editor, model, entityCount);
            }
        }

        @Override
        public void remove(Optional<Value> removedValue, Path pathToField, Document afterPatch, Editor<Patch<EntityType>> editor,
                           EntityCollection model, DeltaCount entityCount) {
            if (removedValue.isPresent() && removedValue.get().isDocument()) {
                removedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), pathToField.append(path), afterPatch, editor, model, entityCount);
                });
            } else {
                delegate.remove(removedValue, pathToField, afterPatch, editor, model, entityCount);
            }
        }

        @Override
        public void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount) {
            // If the replaced value is complex, it must be removed piece by piece ...
            if (replacedValue.isPresent() && replacedValue.get().isDocument()) {
                replacedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), toPath.append(path), afterPatch, editor, model, entityCount);
                });
                replacedValue = Optional.empty();
            }
            // If the copied value is complex, it must be added piece by piece ...
            if (copiedValue.isPresent() && copiedValue.get().isDocument()) {
                if (replacedValue.isPresent()) {
                    // We didn't yet remove the (simple) replaced value ...
                    delegate.remove(replacedValue, toPath, afterPatch, editor, model, entityCount);
                }
                Document doc = copiedValue.get().asDocument();
                // Add each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.add(Optional.empty(), toPath.append(path), value, afterPatch, editor, model, entityCount);
                });
            } else {
                // The copied value is not complex, so just delegate ...
                delegate.copy(fromPath, copiedValue, toPath, replacedValue, afterPatch, editor, model, entityCount);
            }
        }

        @Override
        public void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount) {
            // If the replaced value is complex, it must be removed piece by piece ...
            if (replacedValue.isPresent() && replacedValue.get().isDocument()) {
                replacedValue.get().asDocument().forEach((path, value) -> {
                    delegate.remove(Optional.ofNullable(value), toPath.append(path), afterPatch, editor, model, entityCount);
                });
                replacedValue = Optional.empty();
            }
            // If the moved value is complex, it must be added piece by piece ...
            if (movedValue.isPresent() && movedValue.get().isDocument()) {
                if (replacedValue.isPresent()) {
                    // We didn't yet remove the (simple) replaced value ...
                    delegate.remove(replacedValue, toPath, afterPatch, editor, model, entityCount);
                }
                Document doc = movedValue.get().asDocument();
                // Add each of the individual values ...
                doc.forEach((path, value) -> {
                    delegate.move(fromPath.append(path), Optional.ofNullable(value), toPath.append(path), Optional.empty(), afterPatch,
                                  editor, model, entityCount);
                });
                // finally remove the top-level moved value from it's old place ...
                delegate.remove(Optional.of(Value.create(Document.create())), fromPath, afterPatch, editor, model, entityCount);
            } else {
                // The moved value is not complex, so just delegate ...
                delegate.move(fromPath, movedValue, toPath, replacedValue, afterPatch, editor, model, entityCount);
            }
        }
    }

    protected static class SimpleFieldStrategy implements Strategy {
        private final FieldUsage fieldUsage;

        public SimpleFieldStrategy(FieldUsage fieldUsage) {
            this.fieldUsage = fieldUsage;
            assert this.fieldUsage != null;
        }

        @Override
        public void add(Optional<Value> beforeValue, Path pathToField, Value after, Document afterPatch, Editor<Patch<EntityType>> editor,
                        EntityCollection model, DeltaCount entityCount) {
            // See if the field is already in the entity before this operation is applied ...
            DeltaCount fieldCount = null;
            if (beforeValue.isPresent()) {
                // There is a before value ...
                if (beforeValue.equals(after)) {
                    // The before and after values are the same, so there's nothing to do ...
                    return;
                }
                if (Value.isNull(after)) {
                    // There is no longer a value after, which means we're removing it ...
                    fieldCount = fieldUsage.markRemoved(pathToField);
                }
            } else {
                // There is no before value ...
                if (Value.isNull(after)) {
                    // The before and after values are both null, so there's nothing to do ...
                    return;
                }
                // There is now a value after, which means we're adding it ...
                fieldCount = fieldUsage.markAdded(pathToField);
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
                if (fieldCount != null) setUsageIfNeeded(field.get(), fieldCount, entityCount, () -> fieldEditor);
            } else {
                // Add the field definition with a best-guess of the type based upon the value ...
                FieldType bestType = determineBestFieldType(after);
                fieldEditor.type(bestType);
                // Update the field's usage statistics ...
                if (fieldCount != null) fieldEditor.usage(fieldCount, entityCount);
            }
        }

        @Override
        public void remove(Optional<Value> beforeValue, Path pathToField, Document afterPatch, Patch.Editor<Patch<EntityType>> editor,
                           EntityCollection model, DeltaCount entityCount) {
            model.field(pathToField).ifPresent(field -> {
                if (!field.isEmpty()) {
                    // The field is defined on the type, so see if the field is in the entity before this operation is applied ...
                    // We only need to do something if the before state had a field to remove ...
                    beforeValue.ifPresent(value -> {
                        // Update the field's usage statistics ...
                        DeltaCount fieldCount = fieldUsage.markRemoved(pathToField);
                        SchemaEditor.editField(editor, pathToField).usage(fieldCount, entityCount);
                    });
                }
            });
        }

        @Override
        public void move(Path fromPath, Optional<Value> movedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Patch.Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount) {
            DeltaCount fieldCount = null;
            boolean movedValueExists = movedValue.isPresent() && !Value.isNull(movedValue.get());
            if (movedValueExists) {
                // There is a value being (re)moved ...
                fieldCount = fieldUsage.markRemoved(fromPath);
            }

            // Make sure the 'from' is marked as optional ...
            Optional<FieldDefinition> toField = model.field(toPath);
            Optional<FieldDefinition> fromField = model.field(fromPath);
            if (fromField.isPresent()) {
                if (!toField.isPresent()) {
                    // Copy the field definition ...
                    SchemaEditor.copyField(editor, fromPath, toPath);
                }
                // Update the field's usage statistics ...
                setUsageIfNeeded(fromField.get(), fieldCount, entityCount, () -> SchemaEditor.editField(editor, fromPath));
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
                // Update the field's usage statistics ...
                fieldCount = fieldUsage.markAdded(toPath);
                setUsageIfNeeded(toField.orElse(null), fieldCount, entityCount, () -> fieldEditor);
            } else {
                // The new value is null ...
                replacedValue.ifPresent(oldValue -> {
                    if (Value.notNull(oldValue)) {
                        toField.ifPresent(field -> {
                            // Update the field's usage statistics ...
                            DeltaCount fieldCounter = fieldUsage.markRemoved(toPath);
                            setUsageIfNeeded(toField.orElse(null), fieldCounter, entityCount, () -> fieldEditor);
                        });
                    }
                });
            }
        }

        @Override
        public void copy(Path fromPath, Optional<Value> copiedValue, Path toPath, Optional<Value> replacedValue, Document afterPatch,
                         Patch.Editor<Patch<EntityType>> editor, EntityCollection model, DeltaCount entityCount) {
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
                DeltaCount fieldCount = fieldUsage.markAdded(toPath);
                setUsageIfNeeded(toField.get(), fieldCount, entityCount, () -> fieldEditor);
            } else {
                // The copied value is null ...
                replacedValue.ifPresent(oldValue -> {
                    if (Value.notNull(oldValue)) {
                        DeltaCount fieldCount = fieldUsage.markRemoved(toPath);
                        setUsageIfNeeded(toField.get(), fieldCount, entityCount, () -> fieldEditor);
                    }
                });
            }
        }
    }
}
