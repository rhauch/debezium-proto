/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.debezium.annotation.NotThreadSafe;
import org.debezium.annotation.ThreadSafe;
import org.debezium.message.Batch;
import org.debezium.message.Batch.Builder;
import org.debezium.message.Document;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Action;
import org.debezium.message.Patch.Editor;
import org.debezium.message.Value;
import org.debezium.model.Entity;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.Identifier;

/**
 * Logic for generating large numbers of {@link Batch batches} that create, modified, and delete entities based upon
 * random selections of field names and values.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public final class RandomContent {

    private static final AtomicLong GENERATED_ID = new AtomicLong(0);

    /**
     * A generator of entity IDs.
     */
    @ThreadSafe
    public static interface IdGenerator {
        /**
         * Generate an array of IDs that are to be edited.
         * 
         * @return the entity IDs; never null
         */
        EntityId[] generateEditableIds();

        /**
         * Generate an array of IDs that are to be removed.
         * 
         * @return the entity IDs; never null
         */
        EntityId[] generateRemovableIds();
    }

    /**
     * Create an {@link IdGenerator} that generates random numbers of editable and removable {@link EntityId}s, where all of the
     * resulting {@link EntityId}s are for the given {@link EntityType}.
     * 
     * @param minimum the minimum number of editable or removable IDs; must not be negative
     * @param maximum the maximum number of editable or removable IDs; must be equal to or greater than {@code minimum}
     * @param type the entity type to use in the generated {@link EntityId}s
     * @return the generator; never null
     */
    public IdGenerator createIdGenerator(int minimum, int maximum, EntityType type) {
        return createIdGenerator(minimum, maximum, Sequences.randomlySelect(type, (EntityType[]) null));
    }

    /**
     * Create an {@link IdGenerator} that generates random numbers of editable and removable {@link EntityId}s, where all of the
     * resulting {@link EntityId}s are for randomly selected {@link EntityType}s.
     * 
     * @param minimum the minimum number of editable or removable IDs; must not be negative
     * @param maximum the maximum number of editable or removable IDs; must be equal to or greater than {@code minimum}
     * @param type the first entity type to randomly select from when generating {@link EntityId}s
     * @param additionalTypes the additional entity types to randomly select from when generating {@link EntityId}s
     * @return the generator; never null
     */
    public IdGenerator createIdGenerator(int minimum, int maximum, EntityType type, EntityType... additionalTypes) {
        return createIdGenerator(minimum, maximum, Sequences.randomlySelect(type, additionalTypes));
    }

    /**
     * Create an {@link IdGenerator} that generates random numbers of editable and removable {@link EntityId}s, where all of the
     * resulting {@link EntityId}s are for randomly selected {@link EntityType}s.
     * 
     * @param minimum the minimum number of editable or removable IDs; must not be negative
     * @param maximum the maximum number of editable or removable IDs; must be equal to or greater than {@code minimum}
     * @param typeSupplier the function that returns entity types used when generating {@link EntityId}s; may not be null
     * @return the generator; never null
     */
    public IdGenerator createIdGenerator(int minimum, int maximum, Supplier<EntityType> typeSupplier) {
        if (minimum < 0) throw new IllegalArgumentException("The minimum value may not be negative");
        if (minimum > maximum) throw new IllegalArgumentException("The minimum value may not be greater than the maximum");
        Random rng = new Random(System.currentTimeMillis());
        int count = maximum - minimum;
        return generateIds(minimum + rng.nextInt(count), minimum + rng.nextInt(count), typeSupplier);
    }

    /**
     * A generator of batch content.
     */
    @NotThreadSafe
    public interface ContentGenerator {
        /**
         * Generate a batch that edits the entities with the supplied IDs.
         * 
         * @param editedIds the identifiers of the entities to be modified/created; may be null if no entities are to be
         *            modified/created
         * @param removedIds the identifiers of the entities to be deleted; may be null if no entities are to be removed
         * @return the batch; never null
         */
        public Batch<EntityId> generateBatch(EntityId[] editedIds, EntityId[] removedIds);

        /**
         * Generate a batch that edits and removes a random number of entities.
         * 
         * @param idGenerator the {@link IdGenerator} instance that should be used to randomly generate IDs; may be null if
         *            an empty batch is to be created
         * @return the batch; never null
         */
        default public Batch<EntityId> generateBatch(IdGenerator idGenerator) {
            if (idGenerator == null) return generateBatch(null, null);
            return generateBatch(idGenerator.generateEditableIds(), idGenerator.generateRemovableIds());
        }

        /**
         * Generate a batch that performs a specified number of edits and removes.
         * 
         * @param numEdits the number of create/edit patches to be added to the batch
         * @param numRemoves the number of delete patches to be added to the batch
         * @param type the entity type for the patches; may not be null
         * @return the batch; never null
         */
        default public Batch<EntityId> generateBatch(int numEdits, int numRemoves, EntityType type) {
            return generateBatch(generateIds(numEdits, numRemoves, type));
        }

        /**
         * Generate an entity representation.
         * 
         * @param id the identifier of the entity; may not be null
         * @return the entity representation; never null
         */
        default public Entity generateEntity(EntityId id) {
            if (id == null) throw new IllegalArgumentException("The entity ID may not be null");
            Batch<EntityId> batch = generateBatch(new EntityId[] { id }, null);
            Patch<EntityId> patch = batch.patch(0);
            Document doc = Document.create();
            patch.apply(doc, (path, op) -> {
            });
            return Entity.with(id, doc);
        }

        /**
         * Add the specified number of edits and specified number of removes to the given {@link Builder}.
         * 
         * @param builder the batch builder; never null
         * @param numEdits the number of edits (including creates); must be greater than or equal to 0
         * @param numRemoves the number of removes; must be greater than or equal to 0
         * @param type the type of entity to create; may not be null
         * @return the supplied batch builder; never null
         */
        default public Builder<EntityId> addToBatch(Builder<EntityId> builder, int numEdits, int numRemoves, EntityType type) {
            IdGenerator generator = generateIds(numEdits, numRemoves, type);
            generateBatch(generator).forEach(builder::patch);
            return builder;
        }
    }

    private static IdGenerator generateIds(int editCount, int removeCount, EntityType type) {
        return generateIds(editCount, removeCount, () -> type);
    }

    private static IdGenerator generateIds(int editCount, int removeCount, Supplier<EntityType> typeSupplier) {
        return new IdGenerator() {
            @Override
            public EntityId[] generateEditableIds() {
                return generateIds(editCount, typeSupplier);
            }

            @Override
            public EntityId[] generateRemovableIds() {
                return generateIds(removeCount, typeSupplier);
            }

            private EntityId[] generateIds(int count, Supplier<EntityType> typeSupplier) {
                if (count <= 0) return null;
                EntityId[] ids = new EntityId[count];
                for (int i = 0; i != count; ++i) {
                    EntityType type = typeSupplier.get();
                    if (type == null) throw new IllegalArgumentException("The entity type may not be null");
                    ids[i] = Identifier.of(type, Long.toString(GENERATED_ID.incrementAndGet()));
                }
                return ids;
            }
        };
    }

    /**
     * Load the random content data from the {@code load-data.txt} file on the classpath.
     * 
     * @return the random content data; never null
     */
    public static RandomContent load() {
        return load("load-data.txt", RandomContent.class);
    }

    /**
     * Load the random content data from the specified file on the classpath.
     * 
     * @param pathToResource the path to the classpath resource that is to be loaded; may not be null
     * @param clazz the class whose class loader will be used to load the resource; if null, this class' class loader will be used
     * @return the random content data; never null
     */
    public static RandomContent load(String pathToResource, Class<?> clazz) {
        return load(pathToResource, () -> clazz != null ? clazz.getClassLoader() : RandomContent.class.getClassLoader());
    }

    /**
     * Load the random content data from the specified file on the classpath.
     * 
     * @param pathToResource the path to the classpath resource that is to be loaded; may not be null
     * @param classLoader the class loader will be used to load the resource; if null, this class' class loader will be used
     * @return the random content data; never null
     */
    public static RandomContent load(String pathToResource, ClassLoader classLoader) {
        return load(pathToResource, () -> classLoader != null ? classLoader : null);
    }

    /**
     * Load the random content data from the specified file on the classpath.
     * 
     * @param pathToResource the path to the classpath resource that is to be loaded; may not be null
     * @param classLoaderSupplier the supplier of the class loader that will be used to load the resource; if null or if it
     *            returns null, this class' class loader will be used
     * @return the random content data; never null
     */
    public static RandomContent load(String pathToResource, Supplier<ClassLoader> classLoaderSupplier) {
        try {
            ClassLoader classLoader = classLoaderSupplier != null ? classLoaderSupplier.get() : null;
            if (classLoader == null) classLoader = RandomContent.class.getClassLoader();
            Reader reader = new Reader();
            IoUtil.readLines(pathToResource, classLoader, null, reader::process);
            return new RandomContent(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load the random content data from the file at the specified Path.
     * 
     * @param path the path of the data file; may not be null
     * @return the random content data; never null
     */
    public static RandomContent load(Path path) {
        if (path == null) throw new IllegalArgumentException("Unable to load content data from a null path");
        try {
            Reader reader = new Reader();
            IoUtil.readLines(path, reader::process);
            return new RandomContent(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Reader {
        private List<FieldValues> fieldValues = new ArrayList<>();
        private FieldValues current = null;

        public void process(String line) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                current = null;
            } else {
                if (current != null && Character.isWhitespace(line.charAt(0))) {
                    current.addValue(trimmed);
                } else {
                    current = new FieldValues(trimmed);
                    fieldValues.add(current);
                }
            }
        }

        public FieldValues[] content() {
            return fieldValues.stream().toArray(FieldValues[]::new);
        }
    }

    private static class FieldValues {
        public final String fieldName;
        private final List<String> values = new ArrayList<>();

        FieldValues(String fieldName) {
            this.fieldName = fieldName;
        }

        void addValue(String value) {
            this.values.add(value);
        }

        String get(Random rng) {
            return values.get(rng.nextInt(values.size()));
        }
    }

    protected final FieldValues[] values;
    private volatile int generated;
    private final long initialSeed = System.currentTimeMillis();
    private final Random fieldCountSelector = new Random(initialSeed);
    private final int minFields;
    private final int maxFields;
    private final Action[] actions = new Action[] {
            Action.ADD, Action.ADD, Action.ADD, Action.ADD, Action.ADD,
            Action.ADD, Action.ADD, Action.ADD, Action.ADD, Action.ADD,
            Action.ADD, Action.ADD, Action.ADD, Action.ADD, Action.ADD,
            Action.REMOVE, Action.REMOVE,
            // Action.COPY,
            // Action.MOVE,
            // Action.REPLACE
    };
    private final int numValues;
    private final int numActions = actions.length;

    private RandomContent(Reader reader) {
        this(reader, 4, 6);
    }

    private RandomContent(Reader reader, int minFields, int maxFields) {
        this.values = reader.content();
        this.maxFields = Math.min(maxFields, this.values.length);
        this.minFields = Math.min(minFields, this.maxFields);
        assert this.maxFields > 0;
        assert this.minFields > 0;
        assert this.maxFields >= this.minFields;
        assert this.values != null;
        this.numValues = this.values.length;
    }

    /**
     * Create a new content generator. Generators are not thread-safe, so this method should be called to create a new
     * content generator for each thread.
     * 
     * @return the new content generator; never null
     */
    public ContentGenerator createGenerator() {
        return createGenerator(minFields, maxFields);
    }

    /**
     * Create a new content generator. Generators are not thread-safe, so this method should be called to create a new
     * content generator for each thread.
     * 
     * @param minFields the minimum number of fields to include in each entity; must be positive
     * @param maxFields the maximum number of fields to include in each entity; must be greater or equal to {@code minFields}
     * @return the new content generator; never null
     */
    public ContentGenerator createGenerator(int minFields, int maxFields) {
        ++generated;
        int fieldCount = minFields + fieldCountSelector.nextInt(maxFields - minFields + 1);
        return new ContentGenerator() {
            Random rng = new Random(initialSeed * generated);

            @Override
            public Batch<EntityId> generateBatch(EntityId[] editedIds, EntityId[] removedIds) {
                Batch.Builder<EntityId> builder = Batch.entities();
                if (editedIds != null) {
                    for (int j = 0; j != editedIds.length; ++j) {
                        Editor<Batch.Builder<EntityId>> editor = builder.edit(editedIds[j]);
                        for (int i = 0; i != fieldCount; ++i) {
                            FieldValues vals = randomField();
                            String fieldName = vals.fieldName;
                            switch (actions[rng.nextInt(numActions)]) {
                                case ADD:
                                    editor.add(fieldName, Value.create(vals.get(rng)));
                                    break;
                                case REMOVE:
                                    editor.remove(fieldName);
                                    break;
                                case COPY:
                                    editor.copy(fieldName, randomField().fieldName);
                                    break;
                                case MOVE:
                                    editor.move(fieldName, randomField().fieldName);
                                    break;
                                case REPLACE:
                                    editor.replace(fieldName, Value.create(vals.get(rng)));
                                    break;
                                case INCREMENT:
                                    editor.increment(fieldName, 1);
                                    break;
                                case REQUIRE:
                                    editor.require(fieldName, Value.create(vals.get(rng)));
                                    break;
                            }
                        }
                        editor.end();
                    }
                }
                if (removedIds != null) {
                    for (int j = 0; j != removedIds.length; ++j) {
                        builder.remove(removedIds[j]);
                    }
                }
                return builder.build();
            }

            private FieldValues randomField() {
                return values[rng.nextInt(numValues)];
            }
        };
    }

}
