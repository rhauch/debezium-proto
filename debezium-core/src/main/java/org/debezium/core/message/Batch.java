/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.ZoneId;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Patch.Editor;
import org.debezium.core.util.Iterators;

/**
 * A set of {@link Patch patches} each applied to a target.
 * @param <IdType> the type of identifier that can be addressed with the batch
 * @author Randall Hauch
 */
public final class Batch<IdType extends Identifier> implements Iterable<Patch<IdType>> {

    /**
     * Create a builder that can be used to record patches on multiple targets.
     * @return the builder for creating a batch; never null
     * @param <T> the type of identifier that can be addressed with the batch
     */
    public static <T extends Identifier> Builder<T> create() {
        return new BatchBuilder<T>();
    }
    
    /**
     * An interface for building a batch of multiple patches.
     * @param <IdType> the type of identifier that can be addressed with the batch
     */
    public static interface Builder<IdType extends Identifier> {
        /**
         * Begin creating a new target object with the given identifier. When the editing is {@link Patch.Editor#end() completed}, the changes will be recorded
         * as an additional patch in the batch.
         * @param id the identifier of the new target object; may not be null
         * @return an editor for the new document; never null
         */
        Patch.Editor<Builder<IdType>> create( IdType id );
        /**
         * Begin editing a target object. When the editing is {@link Patch.Editor#end() completed}, the changes will be recorded
         * as an additional patch in the batch.
         * @param id the identifier of the target object; may not be null
         * @return an editor for the new document; never null
         */
        Patch.Editor<Builder<IdType>> edit( IdType id );
        /**
         * Record the removal of the target object with the given identifier. This method immediately adds the patch to the
         * batch.
         * @param id the identifier of the target object; may not be null
         * @return this batch builder instance to easily chain together multiple method invocations on the builder; never null
         */
        Builder<IdType> remove( IdType id );
        /**
         * Complete and return the batch. The builder can be used to create additional batches after this.
         * @return the new batch; never null
         */
        Batch<IdType> build();
    }
    
    protected static final class BatchBuilder<T extends Identifier> implements Builder<T> {
        
        protected List<Patch<T>> patches = new LinkedList<>();
        private final PatchEditor editor = new PatchEditor();
        @Override
        public Patch.Editor<Builder<T>> create(T target) {
            if ( patches == null ) patches = new LinkedList<>();
            return editor.edit(target);
        }
        @Override
        public Patch.Editor<Builder<T>> edit(T target) {
            if ( patches == null ) patches = new LinkedList<>();
            return editor.edit(target);
        }
        @Override
        public Builder<T> remove(T target) {
            if ( patches == null ) patches = new LinkedList<>();
            return editor.remove(target);
        }
        @Override
        public Batch<T> build() {
            try {
            return new Batch<T>(patches);
            } finally {
                patches = null;
            }
        }
    
        protected final class PatchEditor implements Patch.Editor<Builder<T>> {
            private Patch.Editor<Patch<T>> patchEditor;
            
            protected PatchEditor edit( T id ) {
                patchEditor = Patch.edit(id);
                return this;
            }
            protected Builder<T> remove( T id ) {
                patchEditor = Patch.edit(id);
                patchEditor.remove("/");
                patches.add(patchEditor.end());
                patchEditor = null;
                return BatchBuilder.this;
            }
            @Override
            public Builder<T> end() {
                patches.add(patchEditor.end());
                patchEditor = null;
                return BatchBuilder.this;
            }
            @Override
            public Optional<Builder<T>> endIfChanged() {
                Optional<Patch<T>> patch = patchEditor.endIfChanged();
                patch.ifPresent((p)->{
                    patches.add(p);
                    patchEditor = null;
                });
                return Optional.of(BatchBuilder.this);
            }

            @Override
            public PatchEditor add(String path, Value value) {
                patchEditor.add(path, value);
                return this;
            }
            @Override
            public PatchEditor remove(String path) {
                patchEditor.remove(path);
                return this;
            }
            @Override
            public PatchEditor replace(String path, Value newValue) {
                patchEditor.replace(path, newValue);
                return this;
            }
            @Override
            public PatchEditor copy(String fromPath, String toPath) {
                patchEditor.copy(fromPath, toPath);
                return this;
            }
            @Override
            public PatchEditor move(String fromPath, String toPath) {
                patchEditor.move(fromPath, toPath);
                return this;
            }
            @Override
            public PatchEditor require(String path, Value expectedValue) {
                patchEditor.require(path, expectedValue);
                return this;
            }
            @Override
            public Editor<Builder<T>> increment(String path, Number increment) {
                patchEditor.increment(path, increment);
                return this;
            }
            @Override
            public String toString() {
                return "Patch editor for batches";
            }
        }
    }
    
    private final List<Patch<IdType>> patches;
    protected Batch( List<Patch<IdType>> patches ) {
        this.patches = patches;
    }
    
    @Override
    public Iterator<Patch<IdType>> iterator() {
        return Iterators.readOnly(patches.iterator());
    }
    public int patchCount() {
        return patches.size();
    }
    
    public Stream<Patch<IdType>> stream() {
        return patches.stream();
    }
    
    @Override
    public String toString() {
        return "[ " + patches.stream().map(Object::toString).collect(Collectors.joining(", ")) + " ]";
    }
    public Document asDocument() {
        Array array = Array.create();
        patches.stream().forEach((patch)->{array.add(patch.asDocument());});
        return Document.create("patches",array);
    }
    
    public boolean isEmpty() {
        return patches.isEmpty();
    }
    
    /**
     * Determine if this batch is a request to read one or more targets. In other words, this batch must contain at least
     * one patch, and every patch must be a {@link Patch#isReadRequest() read request}.
     * @return true if this batch contains only read requests, or false otherwise
     */
    public boolean isReadRequest() {
        return !patches.isEmpty() || patches.stream().allMatch((patch)->patch.isReadRequest());
    }

    /**
     * Check that all patches apply to the identified database.
     * @param dbId the database identifier
     * @return true if the batch contents all apply to the database, or false otherwise
     */
    public boolean appliesTo( DatabaseId dbId ) {
        for ( Patch<IdType> patch : this ) {
            if ( patch.target().isIn(dbId) ) return false;
        }
        return true;
    }
    
    /**
     * Check that all patches apply to the identified entity.
     * @param entityId the entity identifier
     * @return true if the batch contents all apply to the entity, or false otherwise
     */
    public boolean appliesTo( EntityId entityId ) {
        for ( Patch<IdType> patch : this ) {
            if ( patch.target().isIn(entityId) ) return false;
        }
        return true;
    }
    
    /**
     * Check that all patches apply to the identified zone.
     * @param zoneId the zone identifier
     * @return true if the batch contents all apply to the zone, or false otherwise
     */
    public boolean appliesTo( ZoneId zoneId ) {
        for ( Patch<IdType> patch : this ) {
            if ( patch.target().isIn(zoneId) ) return false;
        }
        return true;
    }
    
    public static <IdType extends Identifier> Batch<IdType> from( Document doc ) {
        Array array = doc.getArray("patches");
        if ( array == null ) return null;
        List<Patch<IdType>> patches = new ArrayList<>(array.size());
        array.streamValues().forEach((value)->{
            if ( value.isDocument() ) {
                Patch<IdType> patch = Patch.from(value.asDocument());
                if ( patch != null ) patches.add(patch);
            }
        });
        return new Batch<IdType>(patches);
    }
}
