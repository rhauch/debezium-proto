/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.Iterator;
import java.util.StringJoiner;

import org.debezium.message.Document;
import org.debezium.message.Document.Field;
import org.debezium.util.HashCode;
import org.debezium.util.Iterators;

/**
 * An identifier of an entity within a collection that holds a specific type of entity.
 * 
 * @author Randall Hauch
 */
public final class ZoneId implements Identifier {
    
    private final EntityType type;
    private final String zoneId;
    private final int hc;
    
    ZoneId(EntityType type, String zoneId) {
        this.type = type;
        this.zoneId = zoneId;
        assert this.type != null;
        assert this.zoneId != null;
        this.hc = HashCode.compute(this.type,this.zoneId);
    }
    
    public EntityType type() {
        return type;
    }
    
    public String zoneId() {
        return zoneId;
    }
    
    public DatabaseId databaseId() {
        return type.databaseId();
    }
    
    @Override
    public int hashCode() {
        return hc;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof ZoneId) {
            ZoneId that = (ZoneId) obj;
            return this.type.equals(that.type) && this.zoneId.equals(that.zoneId);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier id) {
        if (this == id) return 0;
        if (id == null) return 1;
        if (id instanceof ZoneId) {
            ZoneId that = (ZoneId) id;
            int diff = this.type.compareTo(that.type);
            if (diff != 0) return diff;
            return this.zoneId.compareTo(that.zoneId);
        }
        if ( id instanceof EntityType ) {
            return 1;  // all EntityType instances are less than ZoneId
        }
        if ( id instanceof DatabaseId ) {
            return 1;  // all Database instances are less than ZoneId
        }
        return -1;
    }
    
    @Override
    public void asString(StringJoiner joiner) {
        type.asString(joiner);
        joiner.add(zoneId);
    }
    
    @Override
    public String toString() {
        return asString();
    }
    
    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field(DATABASE_FIELD_NAME, type.databaseId().asString()),
                              Document.field(ENTITY_TYPE_FIELD_NAME, type.entityTypeName()),
                              Document.field(ZONE_FIELD_NAME, zoneId));
    }
    
    @Override
    public boolean isIn(DatabaseId dbId) {
        return this.type.isIn(dbId);
    }
    
    @Override
    public boolean isIn(EntityType entityType) {
        return this.type.isIn(entityType);
    }
    
    @Override
    public boolean isIn(ZoneId zoneId) {
        return this.equals(zoneId);
    }

    @Override
    public boolean isIn(EntityId entityId) {
        return false;
    }
}
