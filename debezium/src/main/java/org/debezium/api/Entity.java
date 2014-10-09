/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api;

import org.debezium.api.doc.Document;

/**
 * @author Randall Hauch
 *
 */
public final class Entity implements Identified<EntityId> {
    
    public static Entity with( EntityId id, Document doc ) {
        return new Entity(id,doc);
    }
    
    private final EntityId id;
    private final Document doc;
    
    protected Entity( EntityId id, Document doc ) {
        this.id = id;
        this.doc = doc;
    }
    
    @Override
    public EntityId id() {
        return id;
    }
    
    @Override
    public Document document() {
        return doc;
    }

}
