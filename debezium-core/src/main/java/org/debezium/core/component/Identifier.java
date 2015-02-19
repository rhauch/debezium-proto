/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.Iterator;
import java.util.StringJoiner;
import java.util.UUID;

import org.debezium.core.doc.Document.Field;
import org.debezium.core.message.Message;
import org.debezium.core.util.Stringifiable;


/**
 * An identifier.
 * @author Randall Hauch
 */
public interface Identifier extends Stringifiable, Comparable<Identifier> {
    
    public static final String DATABASE_FIELD_NAME = Message.Field.DATABASE_ID;
    public static final String ENTITY_TYPE_FIELD_NAME = Message.Field.COLLECTION;
    public static final String ZONE_FIELD_NAME = Message.Field.ZONE_ID;
    public static final String ENTITY_FIELD_NAME = Message.Field.ENTITY;
    
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
        return of(new ZoneId( entityType, zoneId.toString()),entityId);
    }
    
    static EntityId of( ZoneId zone, CharSequence entityId ) {
        return new EntityId(zone,entityId.toString());
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

    static ZoneId zone( CharSequence database, CharSequence entityType, CharSequence zoneId ) {
        return new ZoneId( of(database, entityType), zoneId.toString());
    }
    
    static ZoneId zone( DatabaseId database, CharSequence entityType, CharSequence zoneId ) {
        return new ZoneId( of(database, entityType), zoneId.toString());
    }
    
    static ZoneId zone( EntityType entityType, CharSequence zoneId ) {
        return new ZoneId( entityType, zoneId.toString());
    }
    
    static SubscriptionId newSubscription(DatabaseId databaseId) {
        return new SubscriptionId(databaseId,UUID.randomUUID().toString());
    }

    static SubscriptionId subscription(DatabaseId databaseId, String id) {
        return new SubscriptionId(databaseId,id);
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
    
    default boolean isNotIn( DatabaseId dbId ) {
        return !isIn(dbId);
    }
    
    default boolean isNotIn( EntityType entityType ) {
        return !isIn(entityType);
    }
    
    default boolean isNotIn( ZoneId zoneId ) {
        return !isIn(zoneId);
    }
    
    default boolean isNotIn( EntityId entityId ) {
        return !isIn(entityId);
    }
}
