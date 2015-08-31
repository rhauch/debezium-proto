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
import org.debezium.util.Iterators;

/**
 * An identifier of a database.
 * @author Randall Hauch
 */
public final class DatabaseId implements Identifier {
    
    private final String id;
    
    DatabaseId( String id ) {
        this.id = id;
        assert this.id != null;
    }
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof DatabaseId ) {
            DatabaseId that = (DatabaseId)obj;
            return this.id.equals(that.id);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier that) {
        if ( this == that ) return 0;
        if ( that == null ) return 1;
        if ( that instanceof DatabaseId ) {
            return this.id.compareTo(((DatabaseId)that).id);
        }
        return -1;  // all DatabaseId instances are less than non-DatabaseId
    }
    @Override
    public void asString(StringJoiner joiner) {
        joiner.add(id);
    }
    
    @Override
    public String asString() {
        return id;
    }
    
    @Override
    public String toString() {
        return asString();
    }

    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field(DATABASE_FIELD_NAME,id));
    }
    
    @Override
    public boolean isIn(DatabaseId dbId) {
        return this.equals(dbId);
    }
    
    @Override
    public boolean isIn(EntityType entityType) {
        return false;
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
