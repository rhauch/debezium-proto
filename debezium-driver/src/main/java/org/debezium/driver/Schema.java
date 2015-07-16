/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Map;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.component.EntityCollection;
import org.debezium.core.doc.Document;

/**
 * A local immutable representation of the schema for a database.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface Schema {

    /**
     * Get descriptions of the entity types defined in this schema.
     * 
     * @return the map of entity types keyed by their simple name; never null, but possibly empty
     */
    public Map<String, EntityCollection> entityTypes();

    /**
     * Get the representation of this schema as a JSON document.
     * 
     * @return the document representation of this schema; never null
     */
    public Document asDocument();

}
