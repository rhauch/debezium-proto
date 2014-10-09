/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api.message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.debezium.api.Identifier;
import org.debezium.api.doc.Array;
import org.debezium.api.doc.Document;
import org.debezium.api.doc.Value;
import org.debezium.api.message.Patch.Editor;
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
    public int operationCount() {
        return patches.size();
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
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Batch from( Document doc ) {
        Array array = doc.getArray("patches");
        if ( array == null ) return null;
        List<Patch<?>> patches = new ArrayList<>(array.size());
        array.streamValues().forEach((value)->{
            if ( value.isDocument() ) {
                Patch<?> patch = Patch.from(value.asDocument());
                if ( patch != null ) patches.add(patch);
            }
        });
        return new Batch(patches);
    }
}
