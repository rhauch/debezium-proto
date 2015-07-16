/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Set;
import java.util.stream.Collectors;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.component.EntityId;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Message;

/**
 * An immutable representation of an entity.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface Entity {

    /**
     * The common built-in fields used on some entities.
     */
    public static final class Field {
        public static final String TYPE = Message.Field.ENTITY_TYPE;
        public static final String TAGS = Message.Field.ENTITY_TAGS;
    }

    /**
     * Determine if this entity exists. This is exactly equivalent to {@code !missing()}.
     * 
     * @return {@code true} if this entity exists, or {@code false} otherwise
     * @see #isMissing()
     */
    default public boolean exists() {
        return asDocument() != null;
    }

    /**
     * Determine if this entity is missing. This is exactly equivalent to {@code !exists()}.
     * 
     * @return {@code true} if this entity is missing, or {@code false} otherwise
     * @see #exists()
     */
    default public boolean isMissing() {
        return !exists();
    }

    /**
     * Get the identifier of this entity
     * 
     * @return the entity ID; never null
     */
    public EntityId id();

    /**
     * Get the JSON document representation of this entity, if it {@link #exists() exists}.
     * 
     * @return the document representation, or null if this entity does not {@link #exists() exist}.
     */
    public Document asDocument();

    /**
     * Get the type name of this entity, if it {@link #exists() exists}.
     * 
     * @return the type name, or null if this entity does not {@link #exists() exist}.
     */
    default public String type() {
        return exists() ? asDocument().getString(Field.TYPE) : null;
    }

    /**
     * Get the tags for this entity, if it {@link #exists() exists}.
     * 
     * @return the set of tags, or null if this entity does not {@link #exists() exist}.
     */
    default public Set<String> tags() {
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
    default public boolean hasTag(String tag) {
        return isMissing() || tag == null ? null : asDocument().getArray(Field.TAGS)
                                                               .streamValues()
                                                               .anyMatch(Value.create(tag)::equals);
    }

}
