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
import org.debezium.core.util.Iterators;


/**
 * The identifier of a type of entity.
 * @author Randall Hauch
 */
public final class EntityType implements SchemaComponentId {
    
    private final DatabaseId db;
    private final String entityTypeName;
    
    protected EntityType( DatabaseId db, String entityTypeName ) {
        this.db = db;
        this.entityTypeName = entityTypeName;
        assert this.db != null;
        assert this.entityTypeName != null;
    }

    public DatabaseId databaseId() {
        return db;
    }
    
    public String entityTypeName() {
        return entityTypeName;
    }
    
    @Override
    public int hashCode() {
        return db.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof EntityType ) {
            EntityType that = (EntityType)obj;
            return this.db.equals(that.db) && this.entityTypeName.equals(that.entityTypeName);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier that) {
        if ( this == that ) return 0;
        if ( that == null ) return 1;
        if ( that instanceof EntityType ) {
            EntityType other = (EntityType)that;
            int diff = this.db.compareTo(other.db);
            if ( diff != 0 ) return diff;
            return this.entityTypeName.compareTo(other.entityTypeName);
        }
        if ( that instanceof DatabaseId ) {
            return 1;  // all DatabaseId instances are less than EntityType
        }
        return -1;  //
    }

    @Override
    public void asString(StringJoiner joiner) {
        db.asString(joiner);
        joiner.add(entityTypeName);
    }

    @Override
    public String toString() {
        return asString();
    }
    
    @Override
    public ComponentType type() {
        return ComponentType.ENTITY_TYPE;
    }
    
    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field(DATABASE_FIELD_NAME, db.asString()),
                              Document.field(ENTITY_TYPE_FIELD_NAME, entityTypeName));
    }
    
    @Override
    public boolean isIn(DatabaseId dbId) {
        return this.db.equals(dbId);
    }
    
    @Override
    public boolean isIn(EntityType entityType) {
        return this.equals(entityType);
    }
    
    @Override
    public boolean isIn(ZoneId zoneId) {
        return false;
    }

    @Override
    public boolean isIn(EntityId entityId) {
        return false;
    }
}
