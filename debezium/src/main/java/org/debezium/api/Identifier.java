/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import java.util.Iterator;
import java.util.StringJoiner;
import java.util.UUID;

import org.debezium.api.doc.Document;
import org.debezium.api.doc.Document.Field;
import org.debezium.core.util.Stringifiable;


/**
 * An identifier.
 * @author Randall Hauch
 */
public interface Identifier extends Stringifiable, Comparable<Identifier> {
    
    public static final String DEFAULT_ZONE = "default";

    static DatabaseId of( CharSequence databaseName ) {
        return new DatabaseId(databaseName.toString());
    }
    
    static EntityType of( CharSequence database, CharSequence entityType ) {
        return of( of(database), entityType);
    }
    
    static EntityType of( DatabaseId database, CharSequence entityType ) {
        return new EntityType( database, entityType.toString());
    }
    
    static EntityId of( CharSequence database, CharSequence entityType, CharSequence entityId ) {
        return of( of(database), entityType, entityId);
    }
    
    static EntityId of( CharSequence database, CharSequence entityType, CharSequence entityId, CharSequence zoneId ) {
        return of( of(database), entityType, entityId, zoneId);
    }
    
    static EntityId of( DatabaseId database, CharSequence entityType, CharSequence entityId ) {
        return of( of( database, entityType), entityId);
    }
    
    static EntityId of( DatabaseId database, CharSequence entityType, CharSequence entityId, CharSequence zoneId ) {
        return of( of( database, entityType), entityId, zoneId);
    }
    
    static EntityId of( EntityType entityType, CharSequence entityId ) {
        return of( entityType, entityId, DEFAULT_ZONE);
    }
    
    static EntityId of( EntityType entityType, CharSequence entityId, CharSequence zoneId ) {
        return new EntityId( entityType, entityId.toString(), zoneId.toString());
    }
    
    static EntityId newEntity( CharSequence database, CharSequence entityType ) {
        return newEntity( of(database), entityType.toString());
    }
    
    static EntityId newEntity( DatabaseId database, CharSequence entityType ) {
        return newEntity( of( database, entityType));
    }
    
    static EntityId newEntity( EntityType entityType ) {
        return newEntity(entityType,DEFAULT_ZONE);
    }

    static EntityId newEntity( EntityType entityType, CharSequence zoneId ) {
        return new EntityId( entityType, UUID.randomUUID().toString(), zoneId.toString());
    }

    default String asString() {
        StringJoiner joiner = new StringJoiner("/");
        asString(joiner);
        return joiner.toString();
    }
    
    static String asString( Identifier id ) {
        if ( id == null ) {
            throw new IllegalArgumentException("The identifier may not be null");
        }
        return id.asString();
    }
    
    static Identifier parse( CharSequence idString ) {
        if ( idString == null ) {
            throw new IllegalArgumentException("The identifier string may not be null");
        }
        String[] parts = idString.toString().split("/");
        if ( parts.length == 1 ) return of(parts[0]);
        if ( parts.length == 2 ) return of(parts[0],parts[1]);
        return of(parts[0],parts[1],parts[2]);
    }
    
    static Identifier parse( Document doc ) {
        if ( doc == null ) return null;
        String databaseId = doc.getString("database");
        if ( databaseId == null ) return null;
        String entityType = doc.getString("entityType");
        if ( entityType == null ) return of(databaseId);
        String zoneId = doc.getString("zoneId");
        if ( zoneId == null ) return of(databaseId,entityType);
        String id = doc.getString("id");
        if ( id == null ) return null;
        return of(databaseId,entityType,id,zoneId);
    }

    Iterator<Field> fields();
}
