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
 * The identifier of a subscription.
 * @author Randall Hauch
 */
public final class SubscriptionId implements Identifier {
    
    private final DatabaseId db;
    private final String id;
    
    protected SubscriptionId( DatabaseId db, String id ) {
        this.db = db;
        this.id = id;
        assert this.db != null;
        assert this.id != null;
    }

    public DatabaseId databaseId() {
        return db;
    }
    
    public String id() {
        return id;
    }
    
    @Override
    public int hashCode() {
        return db.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof SubscriptionId ) {
            SubscriptionId that = (SubscriptionId)obj;
            return this.db.equals(that.db) && this.id.equals(that.id);
        }
        return false;
    }
    
    @Override
    public int compareTo(Identifier that) {
        if ( this == that ) return 0;
        if ( that == null ) return 1;
        if ( that instanceof SubscriptionId ) {
            SubscriptionId other = (SubscriptionId)that;
            int diff = this.db.compareTo(other.db);
            if ( diff != 0 ) return diff;
            return this.id.compareTo(other.id);
        }
        if ( that instanceof DatabaseId ) {
            return 1;  // all DatabaseId instances are less than SubscriptionId
        }
        return -1;
    }

    @Override
    public void asString(StringJoiner joiner) {
        db.asString(joiner);
        joiner.add(id);
    }

    @Override
    public String toString() {
        return asString();
    }
    
    @Override
    public Iterator<Field> fields() {
        return Iterators.with(Document.field(DATABASE_FIELD_NAME, db.asString()),
                              Document.field(ENTITY_TYPE_FIELD_NAME, id));
    }
    
    @Override
    public boolean isIn(DatabaseId dbId) {
        return this.db.equals(dbId);
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
