/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.debezium.message.Document;
import org.debezium.message.Document.Field;
import org.debezium.message.Path;

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
    
    /**
     * Get the representation of this schema as a JSON document.
     * 
     * @return the document representation of this schema; never null
     */
    public Document asDocument() {
        return doc;
    }
    
    /**
     * Get descriptions of the entity types defined in this schema.
     * 
     * @return the map of entity types keyed by their simple name; never null, but possibly empty
     */
    public Map<String, EntityCollection> entityTypes() {
        return collections().collect(Collectors.toMap(col->col.id().entityTypeName(), col->col));
    }

    // public Stream<? extends SchemaComponent<? extends SchemaComponentId>> components() {
    // return collections();
    // //return Stream.concat(collections(), b);
    // }

    /**
     * Get the stream of {@link EntityCollection}s defined in this schema.
     * 
     * @return the stream of entity collections; never null but possibly empty
     */
    public Stream<EntityCollection> collections() {
        return doc.children(COLLECTIONS_PATH).filter(this::isDocumentValue).map(this::toEntityCollection);
    }
    
    protected boolean isDocumentValue( Field field ) {
        return field != null && field.getValue().isDocument();
    }
    
    protected EntityCollection toEntityCollection(Field field) {
        EntityType type = Identifier.of(id, field.getName());
        return new EntityCollection(type,field.getValue().asDocument());
    }
    
}
