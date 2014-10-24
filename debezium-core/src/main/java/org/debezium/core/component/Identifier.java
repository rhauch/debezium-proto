/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.Iterator;
import java.util.StringJoiner;
import java.util.UUID;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.Document.Field;
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
        return new EntityId(new ZoneId( entityType, zoneId.toString()),entityId.toString());
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
        return new EntityId(new ZoneId( entityType, zoneId.toString()),UUID.randomUUID().toString());
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
    
    static Identifier parseIdentifier( Object object ) {
        return parse(object,Identifier.class);
    }

    static DatabaseId parseDatabaseId( Object object ) {
        return parse(object,DatabaseId.class);
    }

    static EntityType parseEntityType( Object object ) {
        return parse(object,EntityType.class);
    }

    static ZoneId parseZoneId( Object object ) {
        return parse(object,ZoneId.class);
    }

    static EntityId parseEntityId( Object object ) {
        return parse(object,EntityId.class);
    }

    static SchemaComponentId parseSchemaComponentId( Object object ) {
        return parse(object,SchemaComponentId.class);
    }

    static <T extends Identifier> T parse( Object obj, Class<T> type ) {
        if ( type.isInstance(obj) ) return type.cast(obj);
        if ( obj instanceof CharSequence ) {
            Identifier id = parse((CharSequence)obj);
            if ( type.isInstance(id) ) return type.cast(id);
        }
        throw new IllegalArgumentException("'" + obj + "' is not a valid entity type");
    }

    static Identifier parse( CharSequence idString ) {
        if ( idString == null ) {
            throw new IllegalArgumentException("The identifier string may not be null");
        }
        String[] parts = idString.toString().split("/");
        assert parts.length != 0;
        if ( parts.length == 1 ) return of(parts[0]);
        if ( parts.length == 2 ) return of(parts[0],parts[1]);
        if ( parts.length == 3 ) return of(parts[0],parts[1],parts[2]);
        return of(parts[0],parts[1],parts[3],parts[2]);
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
        if ( id == null ) return new ZoneId(of(databaseId,entityType),zoneId);
        return of(databaseId,entityType,id,zoneId);
    }

    Iterator<Field> fields();
    
    default boolean isIn( Identifier id ) {
        if ( id instanceof DatabaseId) return isIn((DatabaseId)id);
        if ( id instanceof EntityType) return isIn((EntityType)id);
        if ( id instanceof ZoneId) return isIn((ZoneId)id);
        if ( id instanceof EntityId) return isIn((EntityId)id);
        return false;
    }
    
    boolean isIn( DatabaseId dbId );
    
    boolean isIn( EntityType entityType );
    
    boolean isIn( ZoneId zoneId );
    
    boolean isIn( EntityId entityId );
}
