/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;

import java.util.stream.Stream;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.Document.Field;
import org.debezium.core.doc.Path;

/**
 * @author Randall Hauch
 *
 */
public final class Schema {
    
    private static final Path COLLECTIONS_PATH = Path.parse("collections");
    
    public static Schema with(DatabaseId id, Document doc) {
        if (id == null) throw new IllegalArgumentException("The 'id' parameter may not be null");
        if (doc == null) throw new IllegalArgumentException("The 'doc' parameter may not be null");
        assert doc != null;
        return new Schema(id, doc);
    }
    
    private final DatabaseId id;
    private final Document doc;
    
    private Schema(DatabaseId id, Document doc) {
        this.id = id;
        this.doc = doc;
    }
    
    public DatabaseId id() {
        return id;
    }
    
    public Stream<? extends SchemaComponent<? extends SchemaComponentId>> components() {
        return collections();
        //return Stream.concat(collections(), b);
    }

    public Stream<EntityCollection> collections() {
        return doc.children(COLLECTIONS_PATH).filter(this::isDocument).map(this::toEntityCollection);
    }
    
    protected boolean isDocument( Field field ) {
        return field != null && field.getValue().isDocument();
    }
    
    protected EntityCollection toEntityCollection(Field field) {
        EntityType type = Identifier.of(id, field.getName());
        return new EntityCollection(type,field.getValue().asDocument());
    }
    
}
