/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import org.debezium.core.doc.Document;

/**
 * @author Randall Hauch
 *
 */
public final class EntityCollection implements SchemaComponent<EntityType> {
    
    public static EntityCollection with( EntityType id, Document doc ) {
        return new EntityCollection(id,doc);
    }
    
    private final EntityType id;
    private final Document doc;
    
    protected EntityCollection( EntityType id, Document doc ) {
        this.id = id;
        this.doc = doc;
    }
    
    @Override
    public EntityType id() {
        return id;
    }
    
    @Override
    public Document document() {
        return doc;
    }

}
