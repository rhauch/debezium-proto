/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.Iterator;
import java.util.StringJoiner;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.Document.Field;
import org.debezium.core.util.HashCode;
import org.debezium.core.util.Iterators;

/**
 * An identifier of an entity within a collection that holds a specific type of entity.
 * 
 * @author Randall Hauch
 */
public final class EntityId implements Identifier {
    
    private final ZoneId zoneId;
    private final String id;
    private final int hc;
    
    EntityId(ZoneId zoneId, String entityId) {
        this.zoneId = zoneId;
        this.id = entityId;
        assert this.zoneId != null;
        assert this.id != null;
        this.hc = HashCode.compute(this.zoneId,this.id);
    }
    
    public ZoneId zoneId() {
        return zoneId;
    }
    
    public String id() {
        return id;
    }
    
    public DatabaseId databaseId() {
        return zoneId.databaseId();
    }
    
    public EntityType type() {
        return zoneId.type();
    }
    
    @Override
    public int hashCode() {
        return hc;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof EntityId) {
            EntityId that = (EntityId) obj;
            return this.zoneId.equals(that.zoneId) && this.id.equals(that.id) && this.zoneId.equals(that.zoneId);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier id) {
        if (this == id) return 0;
        if (id == null) return 1;
        if (id instanceof EntityId) {
            EntityId that = (EntityId) id;
            int diff = this.zoneId.compareTo(that.zoneId);
            if (diff != 0) return diff;
            return this.id.compareTo(that.id);
        }
        return 1; // all EntityId instances are less than non-EntityId instances
    }
    
    @Override
    public void asString(StringJoiner joiner) {
        zoneId.asString(joiner);
        joiner.add(id);
    }
    
    @Override
    public String toString() {
        return asString();
    }
    
    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field("database", zoneId.type().databaseId().asString()),
                              Document.field("entityType", zoneId.type().entityTypeName()),
                              Document.field("zone", zoneId.zoneId()),
                              Document.field("id", id));
    }
    
    @Override
    public boolean isIn(DatabaseId dbId) {
        return this.zoneId.isIn(dbId);
    }
    
    @Override
    public boolean isIn(EntityType entityType) {
        return this.zoneId.isIn(entityType);
    }
    
    @Override
    public boolean isIn(ZoneId zoneId) {
        return this.zoneId.isIn(zoneId);
    }

    @Override
    public boolean isIn(EntityId entityId) {
        return this.equals(entityId);
    }
}
