/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import org.debezium.message.Document;

/**
 * @author Randall Hauch
 *
 */
public final class Entity implements Identified<EntityId> {
    
    public static Entity empty( EntityId id ) {
        if ( id == null ) throw new IllegalArgumentException("The 'id' parameter may not be null");
        return new Entity(id,null);
    }
    
    public static Entity with( EntityId id, Document doc ) {
        if ( id == null ) throw new IllegalArgumentException("The 'id' parameter may not be null");
        if ( doc == null ) throw new IllegalArgumentException("The 'doc' parameter may not be null");
        return new Entity(id,doc);
    }
    
    private final EntityId id;
    private final Document doc;
    
    protected Entity( EntityId id, Document doc ) {
        this.id = id;
        this.doc = doc;
    }
    
    public boolean exists() {
        return doc != null;
    }
    
    public boolean isMissing() {
        return doc == null;
    }
    
    @Override
    public EntityId id() {
        return id;
    }
    
    @Override
    public Document document() {
        return doc;
    }
    
    @Override
    public String toString() {
        return id.toString() + "-" + doc;
    }

}
