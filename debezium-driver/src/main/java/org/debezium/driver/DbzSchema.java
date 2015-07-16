/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Map;
import java.util.stream.Collectors;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityCollection;
import org.debezium.core.component.Schema;
import org.debezium.core.doc.Document;

/**
 * @author Randall Hauch
 *
 */
final class DbzSchema implements org.debezium.driver.Schema {

    private final Schema schema;
    
    DbzSchema( DatabaseId dbId, Document schemaDoc ) {
        this.schema = Schema.with(dbId, schemaDoc);
    }

    @Override
    public Map<String, EntityCollection> entityTypes() {
        return schema.collections().collect(Collectors.toMap(col->col.id().entityTypeName(), col->col));
    }

    @Override
    public Document asDocument() {
        return schema.document();
    }

}
