/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import java.util.Iterator;
import java.util.StringJoiner;

import org.debezium.api.doc.Document;
import org.debezium.api.doc.Document.Field;
import org.debezium.core.util.Iterators;

/**
 * An identifier of an entity within a collection that holds a specific type of entity.
 * 
 * @author Randall Hauch
 */
public final class EntityId implements Identifier {
    
    private final EntityType type;
    private final String id;
    private final String zoneId;
    
    EntityId(EntityType type, String entityId, String zoneId) {
        this.type = type;
        this.id = entityId;
        this.zoneId = zoneId;
        assert this.type != null;
        assert this.id != null;
        assert this.zoneId != null;
    }
    
    public EntityType type() {
        return type;
    }
    
    public String id() {
        return id;
    }
    
    public String zoneId() {
        return zoneId;
    }
    
    @Override
    public int hashCode() {
        return type.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof EntityId) {
            EntityId that = (EntityId) obj;
            return this.type.equals(that.type) && this.id.equals(that.id) && this.zoneId.equals(that.zoneId);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier id) {
        if (this == id) return 0;
        if (id == null) return 1;
        if (id instanceof EntityId) {
            EntityId that = (EntityId) id;
            int diff = this.type.compareTo(that.type);
            if (diff != 0) return diff;
            diff = this.zoneId.compareTo(that.zoneId);
            if (diff != 0) return diff;
            return this.id.compareTo(that.id);
        }
        return 1; // all EntityId instances are less than non-EntityId instances
    }
    
    @Override
    public void asString(StringJoiner joiner) {
        type.asString(joiner);
        joiner.add(id);
    }
    
    @Override
    public String toString() {
        return asString();
    }
    
    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field("database", type.getDatabaseId().asString()),
                              Document.field("entityType", type.getEntityTypeName()),
                              Document.field("zone", zoneId),
                              Document.field("id", id));
    }
}
