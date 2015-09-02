/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.debezium.annotation.Immutable;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Value;

/**
 * An immutable representation of an entity.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class Entity {

    /**
     * The common built-in fields used on some entities.
     */
    public static final class Field {
        public static final String TYPE = Message.Field.ENTITY_TYPE;
        public static final String TAGS = Message.Field.ENTITY_TAGS;
    }

    /**
     * Create an entity instance with the given ID and document representation.
     * @param id the ID; may not be null
     * @param doc the representation; may be null if the entity does not exist
     * @return the entity; never null
     */
    public static Entity with(EntityId id, Document doc) {
        if (id == null) throw new IllegalArgumentException("The 'id' parameter may not be null");
        return new Entity(id, doc);
    }

    private final EntityId id;
    private final Document doc;
    
    private Entity( EntityId id, Document doc ) {
        assert id != null;
        this.id = id;
        this.doc = doc;
    }

    /**
     * Determine if this entity exists. This is exactly equivalent to {@code !missing()}.
     * 
     * @return {@code true} if this entity exists, or {@code false} otherwise
     * @see #isMissing()
     */
    public boolean exists() {
        return asDocument() != null;
    }

    /**
     * Determine if this entity is missing. This is exactly equivalent to {@code !exists()}.
     * 
     * @return {@code true} if this entity is missing, or {@code false} otherwise
     * @see #exists()
     */
    public boolean isMissing() {
        return !exists();
    }

    /**
     * Get the identifier of this entity
     * 
     * @return the entity ID; never null
     */
    public EntityId id() {
        return id;
    }

    /**
     * Get the JSON document representation of this entity, if it {@link #exists() exists}.
     * 
     * @return the document representation, or null if this entity does not {@link #exists() exist}.
     */
    public Document asDocument() {
        return doc;
    }

    /**
     * Get the type name of this entity, if it {@link #exists() exists}.
     * 
     * @return the type name, or null if this entity does not {@link #exists() exist}.
     */
    public String type() {
        return exists() ? asDocument().getString(Field.TYPE) : null;
    }

    /**
     * Get the tags for this entity, if it {@link #exists() exists}.
     * 
     * @return the set of tags, or null if this entity does not {@link #exists() exist}.
     */
    public Set<String> tags() {
        return isMissing() ? null : asDocument().getArray(Field.TAGS)
                                                .streamValues()
                                                .map(v -> v.asString())
                                                .collect(Collectors.toSet());
    }

    /**
     * Determine if this entity {@link #exists() exists} and has the given tag.
     * 
     * @param tag the tag for the entity
     * @return the {@code true} if this entity {@link #exists() exists}, the tag is not null, and the entity has the given tag, or
     *         false otherwise
     */
    public boolean hasTag(String tag) {
        return isMissing() || tag == null ? null : asDocument().getArray(Field.TAGS)
                                                               .streamValues()
                                                               .anyMatch(Value.create(tag)::equals);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof Entity ) {
            Entity that = (Entity)obj;
            return this.id.equals(that.id()) && Objects.equals(this.doc, that.asDocument());
        }
        return false;
    }
    
    @Override
    public String toString() {
        return asDocument().toString();
    }
}
