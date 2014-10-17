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

    public DatabaseId getDatabaseId() {
        return db;
    }
    
    public String getEntityTypeName() {
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
    public Iterator<Field> fields() {
        return Iterators.with(Document.field("database", db.asString()),
                              Document.field("entityType", entityTypeName));
    }
}
